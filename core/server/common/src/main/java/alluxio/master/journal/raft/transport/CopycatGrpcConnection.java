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

import com.google.protobuf.ByteString;
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

    mClosed = false;
    mRequestCounter = new AtomicLong(0);
    // Initialize timeout scheduler.
    mTimeoutScheduler = context.schedule(Duration.ofMillis(mRequestTimeoutMs),
        Duration.ofMillis(mRequestTimeoutMs / 2), this::timeout);
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
    return sendAndReceive(request);
  }

  @Override
  public <T, U> CompletableFuture<U> sendAndReceive(T request) {
    Assert.notNull(request, "request");

    // Create a contextual future for the request.
    CopycatGrpcConnection.ContextualFuture<U> future = new CopycatGrpcConnection.ContextualFuture<>(
        System.currentTimeMillis(), ThreadContext.currentContextOrThrow());

    // Get a new request Id.
    long requestId = mRequestCounter.incrementAndGet();

    // Serialize the request and send it over to target.
    try {
      mTargetObserver.onNext(CopycatMessage.newBuilder()
          .setRequestHeader(CopycatRequestHeader.newBuilder().setRequestId(requestId))
          .setMessage(
              ByteString.copyFrom(future.getContext().serializer().writeObject(request).array()))
          .build());
    } catch (Exception e) {
      future.completeExceptionally(e);
      return future;
    }

    // Request is sent over. Store it for later when handling responses.
    mResponseFutures.put(requestId, future);
    LOG.debug("Submitted request: {} for type: {}", requestId, request.getClass().getName());

    return future;
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
    mHandlers.put(type,
        new CopycatGrpcConnection.HandlerHolder(handler, ThreadContext.currentContextOrThrow()));
    return null;
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
    // Send the response.
    if (responseFuture != null) {
      responseFuture.whenComplete((response, error) -> {
        ThreadContext context = ThreadContext.currentContextOrThrow();
        if (context == null) {
          mContext.executor().execute(() -> {
            if (error == null) {
              sendResponse(requestId, mContext, response);
            } else {
              sendResponse(requestId, mContext, error);
            }
          });
        } else {
          if (error == null) {
            sendResponse(requestId, mContext, response);
          } else {
            sendResponse(requestId, mContext, error);
          }
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
    LOG.debug("Sending response of type: {} for request: {}", responseObject.getClass().getName(),
        requestId);
    mTargetObserver.onNext(CopycatMessage.newBuilder()
        .setResponseHeader(CopycatResponseHeader.newBuilder().setRequestId(requestId)
            .setFailed(responseObject instanceof Throwable))
        .setMessage(ByteString.copyFrom(context.serializer().writeObject(responseObject).array()))
        .build());
  }

  protected void handleResponseMessage(CopycatMessage response) {
    CopycatGrpcConnection.ContextualFuture future =
        mResponseFutures.remove(response.getResponseHeader().getRequestId());

    if (future == null) {
      return;
    }

    try {
      if (response.getResponseHeader().getFailed()) {
        Throwable error = mContext.serializer().readObject(response.getMessage().newInput());
        LOG.debug("Received error response for request: {}. Error: {}",
            response.getResponseHeader().getRequestId(), error);
        future.getContext().executor().execute(() -> future.completeExceptionally(error));
      } else {
        Object responseObject = mContext.serializer().readObject(response.getMessage().newInput());
        LOG.debug("Received response of type: {} request: {}.", responseObject.getClass().getName(),
            response.getResponseHeader().getRequestId());
        future.getContext().executor().execute(() -> future
            .complete(responseObject));
      }
    } catch (SerializationException e) {
      future.getContext().executor().execute(() -> future.completeExceptionally(e));
    }
  }

  @Override
  public Listener<Throwable> onException(Consumer<Throwable> listener) {
    // Call immediately if the connection was faulted.
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
      mTargetObserver.onCompleted();

      mClosed = true;
      // Stop timeout timer.
      mTimeoutScheduler.cancel();

      // Close outstanding calls.
      for (CopycatGrpcConnection.ContextualFuture<?> responseFuture : mResponseFutures.values()) {
        responseFuture.getContext().executor().execute(
            () -> responseFuture.completeExceptionally(new ConnectException("Connection closed.")));
      }
      mResponseFutures.clear();

      // Invoke close listeners.
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
    LOG.debug("Connection faulted. Owner: {}. Error: {}", mConnectionOwner, t);
    mLastFailure = t;

    // Close outstanding calls with the failure.
    for (CopycatGrpcConnection.ContextualFuture<?> responseFuture : mResponseFutures.values()) {
      responseFuture.getContext().executor().execute(() -> responseFuture.completeExceptionally(t));
    }
    mResponseFutures.clear();

    // Call exception listeners.
    for (Listener<Throwable> listener : mExceptionListeners) {
      listener.accept(t);
    }
  }

  @Override
  public void onCompleted() {
    LOG.debug("Connection completed. Owner: {}", mConnectionOwner);
    // Server owns client's connection.
    if (mConnectionOwner == ConnectionOwner.SERVER) {
      // Complete the close on client side.
      mTargetObserver.onCompleted();
    }
  }

  /**
   * Times out pending requests.
   */
  void timeout() {
    long currentTimeMillis = System.currentTimeMillis();
    Iterator<Map.Entry<Long, CopycatGrpcConnection.ContextualFuture>> iterator =
        mResponseFutures.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, ContextualFuture> requestEntry = iterator.next();
      CopycatGrpcConnection.ContextualFuture future = requestEntry.getValue();
      if (future.getCreationTime() + mRequestTimeoutMs < currentTimeMillis) {
        LOG.debug("Timing out request: {}", requestEntry.getKey());
        iterator.remove();
        future.getContext().executor().execute(
            () -> future.completeExceptionally(new TimeoutException("Request timed out.")));
      } else {
        break;
      }
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
