/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.journal.raft.transport;

import alluxio.grpc.CopycatMessage;
import alluxio.grpc.CopycatRequestHeader;
import alluxio.grpc.CopycatResponseHeader;
import alluxio.resource.LockResource;

import com.google.protobuf.UnsafeByteOperations;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.concurrent.Listeners;
import io.atomix.catalyst.concurrent.Scheduled;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.serializer.SerializationException;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.Assert;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Copycat transport {@link Connection} implementation that uses Alluxio gRPC.
 */
public class CopycatGrpcConnection implements Connection, StreamObserver<CopycatMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(CopycatGrpcConnection.class);

  /** Exception listeners for this connection.  */
  private final Listeners<Throwable> mExceptionListeners;
  /** Close listeners for this connection.  */
  private final Listeners<Connection> mCloseListeners;

  /** Whether this connection is closed already. */
  private boolean mClosed;

  /** Whether underlying gRPC stream is completed. */
  private boolean mStreamCompleted;

  /** Failure on the connection. */
  private Throwable mLastFailure;

  /** Counter for assigning connection bound requests Ids. */
  private final AtomicLong mRequestCounter;

  /** Map of request handlers. */
  private final Map<Class, CopycatGrpcConnection.HandlerHolder> mHandlers;
  /** Map of pending requests. */
  private final Map<Long, CopycatGrpcConnection.ContextualFuture> mResponseFutures;

  /** Thread context of creator of this connection. */
  private final ThreadContext mContext;

  /** Type of connection owner. */
  private final ConnectionOwner mConnectionOwner;

  /** Target's stream observer. */
  private StreamObserver<CopycatMessage> mTargetObserver;

  /** Time in milliseconds to timeout pending requests.  */
  private final long mRequestTimeoutMs;

  /** Timeout scheduler. */
  private final Scheduled mTimeoutScheduler;

  /** Used to synchronize during connection shut down. */
  private final ReadWriteLock mStateLock;

  /**
   * Creates a connection object.
   *
   * Note: {@link #setTargetObserver} should be called explicitly before using the connection.
   *
   * @param connectionOwner owner of connection
   * @param context copycat thread context
   * @param requestTimeoutMs timeout in milliseconds for requests
   */
  public CopycatGrpcConnection(ConnectionOwner connectionOwner, ThreadContext context,
      long requestTimeoutMs) {
    mConnectionOwner = connectionOwner;
    mContext = context;
    mRequestTimeoutMs = requestTimeoutMs;

    mStateLock = new ReentrantReadWriteLock();
    mClosed = false;
    mStreamCompleted = false;
    mRequestCounter = new AtomicLong(0);
    // Initialize timeout scheduler.
    mTimeoutScheduler = context.schedule(Duration.ofMillis(mRequestTimeoutMs),
        Duration.ofMillis(mRequestTimeoutMs / 2), this::timeoutPendingRequests);
    // Initialize listeners.
    mExceptionListeners = new Listeners<>();
    mCloseListeners = new Listeners<>();
    // Initialize handler and response maps.
    mHandlers = new ConcurrentHashMap<>();
    mResponseFutures = new ConcurrentSkipListMap<>();
  }

  /**
   * Sets the stream observer for target.
   *
   * @param targetObserver target's stream observer
   */
  public void setTargetObserver(StreamObserver<CopycatMessage> targetObserver) {
    mTargetObserver = targetObserver;
  }

  @Override
  public CompletableFuture<Void> send(Object request) {
    return sendAndReceiveInternal(request, true);
  }

  @Override
  public <T, U> CompletableFuture<U> sendAndReceive(T request) {
    return sendAndReceiveInternal(request, false);
  }

  /**
   * Send the request to target. If "fireAndForget" then returned future will be complete.
   *
   * @param request request to send
   * @param fireAndForget whether to not wait for response
   * @param <T> Request type
   * @param <U> Response type
   * @return future for result
   */
  private <T, U> CompletableFuture<U> sendAndReceiveInternal(T request, boolean fireAndForget) {
    try (LockResource lock = new LockResource(mStateLock.readLock())) {
      Assert.notNull(request, "request");

      // Create a contextual future for the request.
      CopycatGrpcConnection.ContextualFuture<U> future =
              new CopycatGrpcConnection.ContextualFuture<>(System.currentTimeMillis(),
                      ThreadContext.currentContextOrThrow());

      // Don't allow request if connection is closed.
      if (mClosed) {
        future.completeExceptionally(new IllegalStateException("Connection closed"));
        return future;
      }

      // Get a new request Id.
      long requestId = mRequestCounter.incrementAndGet();
      // Register request future.
      mResponseFutures.put(requestId, future);

      // Serialize the request and send it over to target.
      try {
        mTargetObserver.onNext(CopycatMessage.newBuilder()
                .setRequestHeader(CopycatRequestHeader.newBuilder().setRequestId(requestId))
                .setMessage(UnsafeByteOperations
                        .unsafeWrap(future.getContext().serializer().writeObject(request).array()))
                .build());
      } catch (Exception e) {
        future.completeExceptionally(e);
        return future;
      }

      // Complete the future if response is not requested.
      if (fireAndForget) {
        future.complete(null);
      }

      // Request is sent over.
      LOG.debug("Submitted request: {} for type: {}. FireAndForget: {}", requestId,
          request.getClass().getName(), fireAndForget);

      return future;
    }
  }

  @Override
  public <T, U> Connection handler(Class<T> type, Consumer<T> handler) {
    return handler(type, r -> {
      handler.accept(r);
      return null;
    });
  }

  @Override
  public <T, U> Connection handler(Class<T> type, Function<T, CompletableFuture<U>> handler) {
    Assert.notNull(type, "type");
    try (LockResource lock = new LockResource(mStateLock.readLock())) {
      // Don't allow request if connection is closed.
      if (mClosed) {
        throw new IllegalStateException("Connection closed");
      }
      mHandlers.put(type,
          new CopycatGrpcConnection.HandlerHolder(handler, ThreadContext.currentContextOrThrow()));
      return null;
    }
  }

  /**
   * Handles request message received from target.
   *
   * @param requestMessage the request message
   */
  private void handleRequestMessage(CopycatMessage requestMessage) {
    // Get request Id.
    long requestId = requestMessage.getRequestHeader().getRequestId();
    try {
      // Deserialize request object.
      Object request = mContext.serializer().readObject(requestMessage.getMessage().newInput());
      LOG.debug("Handling request: {} of type: {}", requestId, request.getClass().getName());
      // Find handler for the request.
      CopycatGrpcConnection.HandlerHolder handler = mHandlers.get(request.getClass());
      if (handler != null) {
        // Handle the request.
        handler.getContext().executor().execute(() -> handleRequest(requestId, request, handler));
      } else {
        // Send fail response.
        sendResponse(requestId, mContext,
            new SerializationException("Unknown message type: " + request.getClass()));
      }
    } catch (SerializationException e) {
      // Send fail response.
      sendResponse(requestId, mContext, e);
    }
  }

  /**
   * Executes the request.
   *
   * @param requestId the request Id
   * @param requestObject the request object
   * @param handler registered handler for the request type
   */
  private void handleRequest(long requestId, Object requestObject,
      CopycatGrpcConnection.HandlerHolder handler) {
    // Call handler for processing the request.
    CompletableFuture<Object> responseFuture = handler.getHandler().apply(requestObject);
    // Send if there is a response.
    if (responseFuture != null) {
      responseFuture.whenComplete((response, error) -> {
        // Action to send response based on future outcome.
        Runnable responseAction = () -> {
          if (error == null) {
            sendResponse(requestId, mContext, response);
          } else {
            sendResponse(requestId, mContext, error);
          }
        };
        // Make sure response is sent under catalyst context.
        if (ThreadContext.currentContext() != null) {
          // Current thread under catalyst context.
          responseAction.run();
        } else {
          // Use originating context for dispatching the response.
          mContext.executor().execute(responseAction);
        }
      });
    }
  }

  /**
   * Sends the response to target.
   *
   * Note: It sends an error response if given object it {@link Throwable}.
   *
   * @param requestId originating request Id
   * @param context copycat thread context
   * @param responseObject response object  to send
   */
  private void sendResponse(long requestId, ThreadContext context, Object responseObject) {
    LOG.debug("Sending response of type: {} for request: {}", responseObjectType(responseObject),
        requestId);
    // Create response message.
    CopycatMessage.Builder messageBuilder =
        CopycatMessage.newBuilder().setResponseHeader(CopycatResponseHeader.newBuilder()
            .setRequestId(requestId).setFailed(responseObject instanceof Throwable));
    // Serialize and embed response object if provided.
    if (responseObject != null) {
      messageBuilder.setMessage(UnsafeByteOperations
          .unsafeWrap(context.serializer().writeObject(responseObject).array()));
    }
    // Send response.
    mTargetObserver.onNext(messageBuilder.build());
  }

  protected void handleResponseMessage(CopycatMessage response) {
    CopycatGrpcConnection.ContextualFuture future =
        mResponseFutures.remove(response.getResponseHeader().getRequestId());

    if (future == null) {
      LOG.debug(
          "Could not find a pending request for id: {}. "
              + "Connection is closed or the request has been timed out.",
          response.getResponseHeader().getRequestId());
      return;
    }

    try {
      if (response.getResponseHeader().getFailed()) {
        Throwable error = mContext.serializer().readObject(response.getMessage().newInput());
        LOG.debug("Received error response for request: {}.",
            response.getResponseHeader().getRequestId(), error);
        future.getContext().executor().execute(() -> future.completeExceptionally(error));
      } else {
        AtomicReference<Object> responseObjectRef = new AtomicReference<>(null);
        if (response.hasMessage()) {
          responseObjectRef.set(mContext.serializer().readObject(response.getMessage().newInput()));
        }
        LOG.debug("Received response of type: {} request: {}.",
            responseObjectType(responseObjectRef.get()),
            response.getResponseHeader().getRequestId());
        // Complete request future on originating context as per interface contract.
        future.getContext().executor().execute(() -> future.complete(responseObjectRef.get()));
      }
    } catch (SerializationException e) {
      future.getContext().executor().execute(() -> future.completeExceptionally(e));
    }
  }

  private String responseObjectType(Object responseObject) {
    return (responseObject != null) ? responseObject.getClass().getName() : "<NULL>";
  }

  @Override
  public Listener<Throwable> onException(Consumer<Throwable> listener) {
    // Call immediately if the connection was failed.
    if (mLastFailure != null) {
      listener.accept(mLastFailure);
    }
    return mExceptionListeners.add(Assert.notNull(listener, "listener"));
  }

  @Override
  public Listener<Connection> onClose(Consumer<Connection> listener) {
    // Call immediately if the connection was closed.
    if (mClosed) {
      listener.accept(this);
    }
    return mCloseListeners.add(Assert.notNull(listener, "listener"));
  }

  @Override
  public CompletableFuture<Void> close() {
    if (!mClosed) {
      LOG.debug("Closing connection. Owner: {}", mConnectionOwner);

      // Connection can't be used after this.
      // Lock and set the state.
      try (LockResource lock = new LockResource(mStateLock.writeLock())) {
        mClosed = true;
      }

      // Stop timeout timer.
      mTimeoutScheduler.cancel();

      // Complete underlying gRPC stream.
      if (!mStreamCompleted) {
        try {
          mTargetObserver.onCompleted();
        } catch (Exception e) {
          LOG.info("Completing underlying gRPC stream failed.", e);
        }
      }

      // Close pending requests.
      failPendingRequests(new ConnectException("Connection closed."));

      // Call close listeners.
      for (Listener<Connection> listener : mCloseListeners) {
        listener.accept(this);
      }
    }

    return CompletableFuture.completedFuture(null);
  }

  /*
   * gRPC message handlers.
   */

  @Override
  public void onNext(CopycatMessage message) {
    LOG.debug("Received message with requestHeader: {}, responseHeader: {}",
        message.getRequestHeader(), message.getResponseHeader());
    // A message can be a request or a response.
    if (message.hasRequestHeader()) {
      handleRequestMessage(message);
    } else if (message.hasResponseHeader()) {
      handleResponseMessage(message);
    } else {
      throw new RuntimeException("Message should contain a request/response header.");
    }
  }

  @Override
  public void onError(Throwable t) {
    LOG.debug("Connection failed. Owner: {}.", mConnectionOwner, t);

    // Connection can't be used after this.
    // Lock and set the state.
    try (LockResource lock = new LockResource(mStateLock.writeLock())) {
      mClosed = true;
    }

    // Used to fail exception listeners immediately until connection is reclaimed.
    mLastFailure = t;

    // Close pending requests.
    failPendingRequests(t);

    // Call exception listeners.
    for (Listener<Throwable> listener : mExceptionListeners) {
      listener.accept(t);
    }

    // Call close listeners as we can't reactivate this connection.
    for (Listener<Connection> listener : mCloseListeners) {
      listener.accept(this);
    }
  }

  @Override
  public void onCompleted() {
    LOG.info("Connection completed. Owner: {}", mConnectionOwner);
    mStreamCompleted = true;
    // Server owns client's stream.
    if (mConnectionOwner == ConnectionOwner.SERVER) {
      // Complete the stream on client side.
      mTargetObserver.onCompleted();
    }

    // Connection can't be used after stream is closed.
    // Close it.
    close();
  }

  /**
   * Times out pending requests.
   */
  private void timeoutPendingRequests() {
    long currentTimeMillis = System.currentTimeMillis();
    Iterator<Map.Entry<Long, CopycatGrpcConnection.ContextualFuture>> responseIterator =
        mResponseFutures.entrySet().iterator();
    while (responseIterator.hasNext()) {
      Map.Entry<Long, ContextualFuture> requestEntry = responseIterator.next();
      CopycatGrpcConnection.ContextualFuture future = requestEntry.getValue();
      if (future.getCreationTime() + mRequestTimeoutMs < currentTimeMillis) {
        LOG.debug("Timing out request: {}", requestEntry.getKey());
        responseIterator.remove();
        future.getContext().executor().execute(
            () -> future.completeExceptionally(new TimeoutException("Request timed out.")));
      } else {
        break;
      }
    }
  }

  /**
   * Closes pending requests with given error.
   *
   * @param error error to close requests with
   */
  private void failPendingRequests(Throwable error) {
    // Close outstanding calls with given error.
    Iterator<Map.Entry<Long, CopycatGrpcConnection.ContextualFuture>> responseFutureIterator =
        mResponseFutures.entrySet().iterator();
    while (responseFutureIterator.hasNext()) {
      Map.Entry<Long, CopycatGrpcConnection.ContextualFuture> responseEntry =
          responseFutureIterator.next();

      LOG.debug("Closing request:{} with error: {}", responseEntry.getKey(),
          error.getClass().getName());

      CopycatGrpcConnection.ContextualFuture<?> responseFuture = responseEntry.getValue();
      responseFuture.getContext().executor()
          .execute(() -> responseFuture.completeExceptionally(error));
    }
  }

  /**
   * Defines the owner of connection.
   */
  protected enum ConnectionOwner {
    CLIENT, // Copycat client.
    SERVER // Copycat server.
  }

  /**
   * Holds message handler and copycat thread context.
   */
  protected static class HandlerHolder {
    /** Request handler. */
    private final Function<Object, CompletableFuture<Object>> mHandler;
    /** Copycat thread context. */
    private final ThreadContext mContext;

    private HandlerHolder(Function handler, ThreadContext context) {
      mHandler = handler;
      mContext = context;
    }

    private ThreadContext getContext() {
      return mContext;
    }

    private Function<Object, CompletableFuture<Object>> getHandler() {
      return mHandler;
    }
  }

  /**
   * A future with copycat thread context.
   */
  protected static class ContextualFuture<T> extends CompletableFuture<T> {
    /** Creation time. */
    private final long mCreationTime;
    /** Copycat thread context. */
    private final ThreadContext mContext;

    private ContextualFuture(long creationTime, ThreadContext context) {
      mCreationTime = creationTime;
      mContext = context;
    }

    private ThreadContext getContext() {
      return mContext;
    }

    private long getCreationTime() {
      return mCreationTime;
    }
  }
}
