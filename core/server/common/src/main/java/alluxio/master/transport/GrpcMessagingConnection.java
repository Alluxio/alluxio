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

package alluxio.master.transport;

import alluxio.grpc.TransportMessage;
import alluxio.grpc.MessagingRequestHeader;
import alluxio.grpc.MessagingResponseHeader;
import alluxio.resource.LockResource;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.protobuf.UnsafeByteOperations;
import io.atomix.catalyst.serializer.SerializationException;
import io.grpc.stub.StreamObserver;
import org.apache.http.concurrent.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Abstract {@link GrpcMessagingConnection} implementation based on Alluxio gRPC messaging.
 */
public abstract class GrpcMessagingConnection
    implements StreamObserver<TransportMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcMessagingConnection.class);

  /** Used to assign a JVM bound Id to connections.  */
  private static AtomicLong sConnectionIdCounter = new AtomicLong(0);

  /** Exception listeners for this connection.  */
  private final Listeners<Throwable> mExceptionListeners;
  /** Close listeners for this connection.  */
  private final Listeners<GrpcMessagingConnection> mCloseListeners;

  /** Whether this connection is closed already. */
  private boolean mClosed;

  /** Whether underlying gRPC stream is completed. */
  private boolean mStreamCompleted;

  /** Failure on the connection. */
  private Throwable mLastFailure;

  /** Counter for assigning connection bound requests Ids. */
  private final AtomicLong mRequestCounter;

  /** Map of request handlers. */
  private final Map<Class, GrpcMessagingConnection.HandlerHolder> mHandlers;
  /** Map of pending requests. */
  private final Map<Long, GrpcMessagingConnection.ContextualFuture> mResponseFutures;

  /** Thread context of creator of this connection. */
  private final GrpcMessagingContext mContext;

  /** Type of connection owner. */
  private final ConnectionOwner mConnectionOwner;

  /** Unique Id of connection. */
  private final String mConnectionId;

  /** Target's stream observer. */
  private StreamObserver<TransportMessage> mTargetObserver;

  /** Time in milliseconds to timeout pending requests.  */
  private final long mRequestTimeoutMs;

  /** Timeout scheduler. */
  private final Cancellable mTimeoutScheduler;

  /** Used to synchronize during connection shut down. */
  private final ReadWriteLock mStateLock;

  /** Executor for connection. */
  private final ExecutorService mExecutor;

  /**
   * Creates a connection object.
   *
   * Note: {@link #setTargetObserver} should be called explicitly before using the connection.
   *
   * @param connectionOwner owner of connection
   * @param transportId transport level Id
   * @param context catalyst thread context
   * @param executor transport executor
   * @param requestTimeoutMs timeout in milliseconds for requests
   */
  public GrpcMessagingConnection(ConnectionOwner connectionOwner, String transportId,
      GrpcMessagingContext context, ExecutorService executor, long requestTimeoutMs) {
    mConnectionOwner = connectionOwner;
    mConnectionId = MoreObjects.toStringHelper(this)
        .add("ConnectionOwner", mConnectionOwner)
        .add("ConnectionId", sConnectionIdCounter.incrementAndGet())
        .add("TransportId", transportId)
        .toString();
    mContext = context;
    mExecutor = executor;
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
  public void setTargetObserver(StreamObserver<TransportMessage> targetObserver) {
    mTargetObserver = targetObserver;
  }

  /**
   * Sends a request.
   *
   * @param request the request to send
   * @return the send request completable future
   */
  public CompletableFuture<Void> send(Object request) {
    return sendAndReceiveInternal(request, true);
  }

  /**
   * Sends a request and receives a response.
   *
   * @param request the request to send
   * @param <T> Request type
   * @param <U> Response type
   * @return future for results
   */
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
      Preconditions.checkNotNull(request, "request should not be null");

      // Create a contextual future for the request.
      GrpcMessagingConnection.ContextualFuture<U> future =
          new GrpcMessagingConnection.ContextualFuture<>(System.currentTimeMillis(),
              GrpcMessagingContext.currentContextOrThrow());

      // Don't allow request if connection is closed.
      if (mClosed) {
        future.completeExceptionally(new IllegalStateException("GrpcMessagingConnection closed"));
        return future;
      }

      // Get a new request Id.
      long requestId = mRequestCounter.incrementAndGet();
      // Register request future.
      mResponseFutures.put(requestId, future);

      // Serialize the request and send it over to target.
      try {
        mTargetObserver.onNext(TransportMessage.newBuilder()
            .setRequestHeader(MessagingRequestHeader.newBuilder().setRequestId(requestId))
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
      LOG.debug("Submitted request({}) of type: {}. Connection: {} FireAndForget: {}", requestId,
          request.getClass().getName(), mConnectionId, fireAndForget);

      return future;
    }
  }

  /**
   * Registers a new message handler.
   *
   * @param type type of the handler
   * @param handler the actual handler to register
   * @param <T> the class and handler type
   * @param <U> the result type
   * @return connection
   */
  public <T, U> GrpcMessagingConnection handler(Class<T> type, Consumer<T> handler) {
    return handler(type, r -> {
      handler.accept(r);
      return null;
    });
  }

  /**
   * Registers a new message handler.
   *
   * @param type type of the handler
   * @param handler the actual handler to register
   * @param <T> the class and handler type
   * @param <U> the result type
   * @return connection
   */
  public <T, U> GrpcMessagingConnection handler(Class<T> type,
      Function<T, CompletableFuture<U>> handler) {
    Preconditions.checkNotNull(type, "type should not be null");
    try (LockResource lock = new LockResource(mStateLock.readLock())) {
      // Don't allow request if connection is closed.
      if (mClosed) {
        throw new IllegalStateException("Connection closed");
      }
      mHandlers.put(type, new GrpcMessagingConnection.HandlerHolder(handler,
          GrpcMessagingContext.currentContextOrThrow()));
      return null;
    }
  }

  /**
   * Handles request message received from target.
   *
   * @param requestMessage the request message
   */
  private void handleRequestMessage(TransportMessage requestMessage) {
    // Get request Id.
    long requestId = requestMessage.getRequestHeader().getRequestId();
    try {
      // Deserialize request object.
      Object request = mContext.serializer().readObject(requestMessage.getMessage().newInput());
      LOG.debug("Handling request({}) of type: {}. GrpcMessagingConnection: {}", requestId,
          request.getClass().getName(), mConnectionId);
      // Find handler for the request.
      GrpcMessagingConnection.HandlerHolder handler = mHandlers.get(request.getClass());
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
      GrpcMessagingConnection.HandlerHolder handler) {
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
        if (GrpcMessagingContext.currentContext() != null) {
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
   * Note: It sends an error response if given object is a {@link Throwable}.
   *
   * @param requestId originating request Id
   * @param context catalyst thread context
   * @param responseObject response object  to send
   */
  private void sendResponse(long requestId, GrpcMessagingContext context, Object responseObject) {
    LOG.debug("Sending response of type: {} for request({}). GrpcMessagingConnection: {}",
        responseObjectType(responseObject), requestId, mConnectionOwner);
    // Create response message.
    TransportMessage.Builder messageBuilder =
        TransportMessage.newBuilder().setResponseHeader(MessagingResponseHeader.newBuilder()
            .setRequestId(requestId).setIsThrowable(responseObject instanceof Throwable));
    // Serialize and embed response object if provided.
    if (responseObject != null) {
      messageBuilder.setMessage(UnsafeByteOperations
          .unsafeWrap(context.serializer().writeObject(responseObject).array()));
    }
    // Send response.
    mTargetObserver.onNext(messageBuilder.build());
  }

  protected void handleResponseMessage(TransportMessage response) {
    GrpcMessagingConnection.ContextualFuture future =
        mResponseFutures.remove(response.getResponseHeader().getRequestId());

    if (future == null) {
      LOG.debug(
          "Received a response for nonexistent request({})."
              + "Connection is closed or the request has been timed out. Connection: {}",
          response.getResponseHeader().getRequestId(), mConnectionId);
      return;
    }

    try {
      if (response.getResponseHeader().getIsThrowable()) {
        Throwable error = mContext.serializer().readObject(response.getMessage().newInput());
        LOG.debug("Received an exception for request({}). Connection: {}",
            response.getResponseHeader().getRequestId(), mConnectionId, error);
        future.getContext().executor().execute(() -> future.completeExceptionally(error));
      } else {
        AtomicReference<Object> responseObjectRef = new AtomicReference<>(null);
        if (response.hasMessage()) {
          responseObjectRef.set(mContext.serializer().readObject(response.getMessage().newInput()));
        }
        LOG.debug("Received response of type: {} for request({}). Connection: {}",
            responseObjectType(responseObjectRef.get()),
            response.getResponseHeader().getRequestId(), mConnectionId);
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

  /**
   * Adds an exception listener.
   *
   * @param listener the listener to add
   * @return the listener holder
   */
  public Listener<Throwable> onException(Consumer<Throwable> listener) {
    // Call immediately if the connection was failed.
    if (mLastFailure != null) {
      listener.accept(mLastFailure);
    }
    return mExceptionListeners.add(
        Preconditions.checkNotNull(listener, "listener should not be null"));
  }

  /**
   * Adds a close listener.
   *
   * @param listener the listener to add
   * @return the listener holder
   */
  public Listener<GrpcMessagingConnection> onClose(
      Consumer<GrpcMessagingConnection> listener) {
    // Call immediately if the connection was closed.
    if (mClosed) {
      listener.accept(this);
    }
    return mCloseListeners.add(Preconditions.checkNotNull(listener, "listener should not be null"));
  }

  /**
   * Closes the connection.
   *
   * @return future of close results
   */
  public CompletableFuture<Void> close() {
    if (mClosed) {
      return CompletableFuture.completedFuture(null);
    }

    return CompletableFuture.runAsync(() -> {
      LOG.debug("Closing connection: {}", mConnectionId);

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
          LOG.debug("Completing underlying gRPC stream failed.", e);
        }
      }

      // Close pending requests.
      failPendingRequests(new ConnectException("Connection closed."));

      // Call close listeners.
      for (Listener<GrpcMessagingConnection> listener : mCloseListeners) {
        listener.accept(this);
      }
    }, mExecutor);
  }

  /*
   * gRPC message handlers.
   */

  @Override
  public void onNext(TransportMessage message) {
    LOG.debug("Received a new message. GrpcMessagingConnection: {}, "
        + "RequestHeader: {}, ResponseHeader: {}",
        mConnectionId, message.getRequestHeader(), message.getResponseHeader());
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
    LOG.debug("Connection failed: {}", mConnectionId, t);

    // GrpcMessagingConnection can't be used after this.
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
    for (Listener<GrpcMessagingConnection> listener : mCloseListeners) {
      listener.accept(this);
    }
  }

  @Override
  public void onCompleted() {
    LOG.debug("Connection completed: {}", mConnectionId);
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

  @Override
  public String toString() {
    return mConnectionId;
  }

  /**
   * Times out pending requests.
   */
  private void timeoutPendingRequests() {
    long currentTimeMillis = System.currentTimeMillis();
    Iterator<Map.Entry<Long, GrpcMessagingConnection.ContextualFuture>> responseIterator =
        mResponseFutures.entrySet().iterator();
    while (responseIterator.hasNext()) {
      Map.Entry<Long, ContextualFuture> requestEntry = responseIterator.next();
      GrpcMessagingConnection.ContextualFuture future = requestEntry.getValue();
      if (future.getCreationTime() + mRequestTimeoutMs < currentTimeMillis) {
        LOG.debug("Timing out request({}). GrpcMessagingConnection: {}", requestEntry.getKey(),
            mConnectionId);
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
    Iterator<Map.Entry<Long, GrpcMessagingConnection.ContextualFuture>> responseFutureIter =
        mResponseFutures.entrySet().iterator();
    while (responseFutureIter.hasNext()) {
      Map.Entry<Long, GrpcMessagingConnection.ContextualFuture> responseEntry =
          responseFutureIter.next();

      LOG.debug("Closing request({}) with error: {}. GrpcMessagingConnection: {}",
          responseEntry.getKey(), error.getClass().getName(), mConnectionId);

      GrpcMessagingConnection.ContextualFuture<?> responseFuture = responseEntry.getValue();
      responseFuture.getContext().executor()
          .execute(() -> responseFuture.completeExceptionally(error));
    }
  }

  /**
   * Defines the owner of connection.
   */
  protected enum ConnectionOwner {
    CLIENT, // Messaging client.
    SERVER // Messaging server.
  }

  /**
   * Holds message handler and catalyst thread context.
   */
  protected static class HandlerHolder {
    /** Request handler. */
    private final Function<Object, CompletableFuture<Object>> mHandler;
    /** Catalyst thread context. */
    private final GrpcMessagingContext mContext;

    private HandlerHolder(Function handler, GrpcMessagingContext context) {
      mHandler = handler;
      mContext = context;
    }

    private GrpcMessagingContext getContext() {
      return mContext;
    }

    private Function<Object, CompletableFuture<Object>> getHandler() {
      return mHandler;
    }
  }

  /**
   * A future with catalyst thread context.
   */
  protected static class ContextualFuture<T> extends CompletableFuture<T> {
    /** Creation time. */
    private final long mCreationTime;
    /** Catalyst thread context. */
    private final GrpcMessagingContext mContext;

    private ContextualFuture(long creationTime, GrpcMessagingContext context) {
      mCreationTime = creationTime;
      mContext = context;
    }

    private GrpcMessagingContext getContext() {
      return mContext;
    }

    private long getCreationTime() {
      return mCreationTime;
    }
  }
}
