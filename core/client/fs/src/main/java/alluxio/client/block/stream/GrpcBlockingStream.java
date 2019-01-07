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

import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.CanceledException;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.Status;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.resource.LockResource;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;

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
  private final StreamObserver<ResT> mResponseObserver;
  private final ClientCallStreamObserver<ReqT> mRequestObserver;
  /** Buffer that stores responses to be consumed by {@link GrpcBlockingStream#receive(long)}. */
  private final BlockingQueue<Object> mResponses;
  private boolean mCompleted = false;
  private boolean mClosed = false;
  private boolean mCanceled = false;

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

  /**
   * @param rpcFunc the gRPC bi-directional stream stub function
   * @param bufferSize maximum number of incoming messages the buffer can hold
   */
  public GrpcBlockingStream(Function<StreamObserver<ResT>, StreamObserver<ReqT>> rpcFunc,
      int bufferSize) {
    mResponses = new ArrayBlockingQueue<>(bufferSize);
    mResponseObserver = new ResponseStreamObserver();
    mRequestObserver = (ClientCallStreamObserver) rpcFunc.apply(mResponseObserver);
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
    if (mClosed || mCanceled) {
      throw new CanceledException("Stream is already closed or canceled.");
    }
    try (LockResource lr = new LockResource(mLock)) {
      while (true) {
        checkError();
        if (mRequestObserver.isReady()) {
          break;
        }
        try {
          if (!mReadyOrFailed.await(timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new DeadlineExceededException(
                String.format("Timeout sending request %s after %dms.", request, timeoutMs));
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new CanceledException(e);
        }
      }
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
      throw new CanceledException("Stream is already canceled.");
    }
    try {
      Object response = mResponses.poll(timeoutMs, TimeUnit.MILLISECONDS);
      if (response == null) {
        throw new DeadlineExceededException(
            String.format("Timeout waiting for response after %dms.", timeoutMs));
      }
      if (response == mResponseObserver) {
        mCompleted = true;
        return null;
      }
      checkError();
      return (ResT) response;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CanceledException(e);
    }
  }

  /**
   * Closes the outbound stream.
   */
  public void close() {
    if (isOpen()) {
      mClosed = true;
      mRequestObserver.onCompleted();
    }
  }

  /**
   * Cancels the stream.
   */
  public void cancel() {
    if (isOpen()) {
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
      ex = GrpcExceptionUtils.fromGrpcStatusException((StatusRuntimeException) t);
      if (ex.getStatus() == Status.CANCELED) {
        // Streams are canceled when server is shutdown. Convert it to UnavailableException for
        // client to retry.
        ex = new UnavailableException("Stream is canceled by server.", ex);
      }
    } else {
      ex = AlluxioStatusException.fromThrowable(mError);
    }
    return ex;
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
        updateException(t);
        mReadyOrFailed.signal();
      }
    }

    @Override
    public void onCompleted() {
      try {
        mResponses.put(this);
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
