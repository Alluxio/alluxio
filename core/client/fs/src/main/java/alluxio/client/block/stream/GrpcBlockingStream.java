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

package alluxio.client.block.stream;

import alluxio.Constants;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.CancelledException;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.UnavailableException;
import alluxio.resource.LockResource;
import alluxio.util.LogUtils;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A helper class for accessing gRPC bi-directional stream synchronously.
 *
 * @param <ReqT> type of the request
 * @param <ResT> type of the response
 */
@NotThreadSafe
public class GrpcBlockingStream<ReqT, ResT> {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcBlockingStream.class);
  private final StreamObserver<ResT> mResponseObserver;
  private final ClientCallStreamObserver<ReqT> mRequestObserver;
  /** Buffer that stores responses to be consumed by {@link GrpcBlockingStream#receive(long)}. */
  private final BlockingQueue<Object> mResponses;
  private final String mDescription;
  private volatile boolean mCompleted = false;
  private volatile boolean mClosed = false;
  private volatile boolean mCanceled = false;

  /**
   * Uses to guarantee the operation ordering.
   *
   * NOTE: {@link StreamObserver} events are async.
   * gRPC worker threads executes the response {@link StreamObserver} events.
   */
  private final ReentrantLock mLock = new ReentrantLock();

  @GuardedBy("mLock")
  private Throwable mError;
  /** This condition is met if mError != null or client is ready to send data. */
  private final Condition mReadyOrFailed = mLock.newCondition();
  /** This is set by the grpc threads, and checked/read by the client. */
  private volatile boolean mClosedFromRemote = false;

  /**
   * @param rpcFunc the gRPC bi-directional stream stub function
   * @param bufferSize maximum number of incoming messages the buffer can hold
   * @param description description of this stream
   */
  public GrpcBlockingStream(Function<StreamObserver<ResT>, StreamObserver<ReqT>> rpcFunc,
      int bufferSize, String description) {
    LOG.debug("Opening stream ({})", description);
    mResponses = new ArrayBlockingQueue<>(bufferSize);
    mResponseObserver = new ResponseStreamObserver();
    mRequestObserver = (ClientCallStreamObserver) rpcFunc.apply(mResponseObserver);
    mDescription = description;
  }

  /**
   * Sends a request. Will wait until the stream is ready before sending or timeout if the
   * given timeout is reached.
   *
   * @param request the request
   * @param timeoutMs maximum wait time before throwing a {@link DeadlineExceededException}
   * @throws IOException if any error occurs
   */
  public void send(ReqT request, long timeoutMs) throws IOException {
    if (mClosed || mCanceled || mClosedFromRemote) {
      throw new CancelledException(formatErrorMessage(
          "Failed to send request %s: stream is already closed or cancelled. clientClosed: %s "
              + "clientCancelled: %s serverClosed: %s",
          LogUtils.truncateMessageLineLength(request), mClosed, mCanceled, mClosedFromRemote));
    }
    try (LockResource lr = new LockResource(mLock)) {
      long startMs = System.currentTimeMillis();
      while (true) {
        checkError();
        if (mRequestObserver.isReady()) {
          break;
        }

        long waitedForMs = System.currentTimeMillis() - startMs;
        if (waitedForMs >= timeoutMs) {
          throw new DeadlineExceededException(formatErrorMessage(
              "Timeout sending request %s after %dms. clientClosed: %s clientCancelled: %s "
                  + "serverClosed: %s",
              LogUtils.truncateMessageLineLength(request), timeoutMs, mClosed, mCanceled,
              mClosedFromRemote));
        }

        try {
          // Wait for a minute max
          long awaitMs = Math.min(timeoutMs - waitedForMs, Constants.MINUTE_MS);
          if (!mReadyOrFailed.await(awaitMs, TimeUnit.MILLISECONDS)) {
            // Log a warning before looping again
            LOG.warn(
                "Stream is not ready for client to send request, will wait again. totalWaitMs: {} "
                    + "clientClosed: {} clientCancelled: {} serverClosed: {} description: {}",
                System.currentTimeMillis() - startMs, mClosed, mCanceled, mClosedFromRemote,
                mDescription);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new CancelledException(
              formatErrorMessage("Failed to send request %s: interrupted while waiting for server.",
                  LogUtils.truncateMessageLineLength(request)), e);
        }
      }
    }
    mRequestObserver.onNext(request);
  }

  /**
   * Sends a request. Will not wait for the stream to be ready. If the stream is closed or cancelled
   * this method will return without an error.
   *
   * @param request the request
   * @throws IOException if any error occurs
   */
  public void send(ReqT request) throws IOException {
    if (mClosed || mCanceled || mClosedFromRemote) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Failed to send request {}: stream is already closed or cancelled. clientClosed: {} "
                + "clientCancelled: {} serverClosed: {} ({})",
            LogUtils.truncateMessageLineLength(request), mClosed, mCanceled, mClosedFromRemote,
            mDescription);
      }
      return;
    }
    try (LockResource lr = new LockResource(mLock)) {
      checkError();
    }
    mRequestObserver.onNext(request);
  }

  /**
   * Receives a response from the server. Will wait until a response is received, or
   * throw an exception if times out.
   *
   * @param timeoutMs maximum time to wait before giving up and throwing
   *                  a {@link DeadlineExceededException}
   * @return the response message, or null if the inbound stream is completed
   * @throws IOException if any error occurs
   */
  public ResT receive(long timeoutMs) throws IOException {
    if (mCompleted) {
      return null;
    }
    if (mCanceled) {
      throw new CancelledException(formatErrorMessage("Stream is already canceled."));
    }

    long startMs = System.currentTimeMillis();
    while (true) {
      long waitedForMs = System.currentTimeMillis() - startMs;
      if (waitedForMs >= timeoutMs) {
        throw new DeadlineExceededException(formatErrorMessage(
            "Timeout waiting for response after %dms. clientClosed: %s clientCancelled: %s "
                + "serverClosed: %s", timeoutMs, mClosed, mCanceled, mClosedFromRemote));
      }

      // Wait for a minute max
      long waitMs = Math.min(timeoutMs - waitedForMs, Constants.MINUTE_MS);
      try {
        Object response = mResponses.poll(waitMs, TimeUnit.MILLISECONDS);
        if (response == null) {
          checkError(); // The stream could have errored while we were waiting
          // Log a warning before looping again
          LOG.warn("Client did not receive message from stream, will wait again. totalWaitMs: {} "
                  + "clientClosed: {} clientCancelled: {} serverClosed: {} description: {}",
              System.currentTimeMillis() - startMs, mClosed, mCanceled, mClosedFromRemote,
              mDescription);
          continue;
        }
        if (response == mResponseObserver) {
          mCompleted = true;
          return null;
        }
        checkError();
        return (ResT) response;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new CancelledException(
            formatErrorMessage("Interrupted while waiting for response."), e);
      }
    }
  }

  /**
   * @return true if the current stream has responses received but hasn't processed
   */
  public boolean hasResponseInCache() {
    return !mResponses.isEmpty();
  }

  /**
   * Closes the outbound stream. If the stream is already closed then invoking this method has no
   * effect.
   */
  public void close() {
    if (isOpen()) {
      LOG.debug("Closing stream ({})", mDescription);
      mClosed = true;
      mRequestObserver.onCompleted();
    }
  }

  /**
   * Cancels the stream. If the stream is already cancelled then invoking this method has no
   * effect.
   */
  public void cancel() {
    if (isOpen()) {
      LOG.debug("Cancelling stream ({})", mDescription);
      mCanceled = true;
      mRequestObserver.cancel("Request is cancelled by user.", null);
    }
  }

  /**
   * Wait for server to complete the inbound stream.
   *
   * @param timeoutMs maximum time to wait for server response
   */
  public void waitForComplete(long timeoutMs) throws IOException {
    if (mCompleted || mCanceled) {
      return;
    }
    while (receive(timeoutMs) != null) {
      // wait until inbound stream is closed from server.
    }
  }

  /**
   * @return whether the stream is closed by the server
   */
  public boolean isClosedFromRemote() {
    return mClosedFromRemote;
  }

  /**
   * @return whether the stream is open
   */
  public boolean isOpen() {
    try (LockResource lr = new LockResource(mLock)) {
      return !mClosed && !mCanceled && mError == null;
    }
  }

  /**
   * @return whether the stream is closed
   */
  public boolean isClosed() {
    return mClosed;
  }

  /**
   * @return whether the stream is canceled
   */
  public boolean isCanceled() {
    return mCanceled;
  }

  private void checkError() throws IOException {
    try (LockResource lr = new LockResource(mLock)) {
      if (mError != null) {
        // prevents rethrowing the same error
        mCanceled = true;
        throw toAlluxioStatusException(mError);
      }
    }
  }

  private AlluxioStatusException toAlluxioStatusException(Throwable t) {
    AlluxioStatusException ex;
    if (t instanceof StatusRuntimeException) {
      ex = AlluxioStatusException.fromStatusRuntimeException((StatusRuntimeException) t);
      if (ex.getStatusCode() == Status.Code.CANCELLED) {
        // Streams are canceled when server is shutdown. Convert it to UnavailableException for
        // client to retry.
        ex = new UnavailableException(formatErrorMessage("Stream is canceled by server."), ex);
      }
    } else {
      ex = AlluxioStatusException.fromThrowable(mError);
    }
    // attaches description to the exception while maintaining the cause
    return (AlluxioStatusException) AlluxioStatusException
        .from(ex.getStatus().withDescription(formatErrorMessage(ex.getMessage())));
  }

  private String formatErrorMessage(String format, Object... args) {
    StringBuilder errorMessage = new StringBuilder(
        format == null ? "Unknown error" : String.format(format, args));
    return new StringBuilder(errorMessage).append(String.format(" (%s)", mDescription)).toString();
  }

  private final class ResponseStreamObserver
      implements ClientResponseObserver<ReqT, ResT> {

    @Override
    public void onNext(ResT response) {
      try {
        mResponses.put(response);
      } catch (InterruptedException e) {
        handleInterruptedException(e);
      }
    }

    @Override
    public void onError(Throwable t) {
      try (LockResource lr = new LockResource(mLock)) {
        LOG.warn("Received error {} for stream ({})", t, mDescription);
        updateException(t);
        mReadyOrFailed.signal();
      }
    }

    @Override
    public void onCompleted() {
      try {
        LOG.debug("Received completed event for stream ({})", mDescription);
        mResponses.put(this);
        mClosedFromRemote = true;
      } catch (InterruptedException e) {
        handleInterruptedException(e);
      }
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<ReqT> requestStream) {
      requestStream.setOnReadyHandler(() -> {
        try (LockResource lr = new LockResource(mLock)) {
          mReadyOrFailed.signal();
        }
      });
    }

    private void handleInterruptedException(InterruptedException e) {
      Thread.currentThread().interrupt();
      try (LockResource lr = new LockResource(mLock)) {
        updateException(e);
      }
      throw new RuntimeException(e);
    }

    /**
     * Updates the channel exception to be the given exception e, or adds e to
     * suppressed exceptions.
     *
     * @param e Exception received
     */
    @GuardedBy("mLock")
    private void updateException(Throwable e) {
      if (mError == null || mError == e) {
        mError = e;
        mResponses.offer(e);
      } else {
        mError.addSuppressed(e);
      }
    }
  }
}
