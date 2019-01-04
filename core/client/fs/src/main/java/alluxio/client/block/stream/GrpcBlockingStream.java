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
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.resource.LockResource;

import io.grpc.Status;
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
  private final BlockingQueue<Object> mResponses = new ArrayBlockingQueue<>(2);
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
  private IOException mError;
  /** This condition is met if mError != null or client is ready to send data. */
  private final Condition mReadyOrFailed = mLock.newCondition();

  /**
   * @param rpcFunc the gRPC bi-directional stream stub function
   */
  public GrpcBlockingStream(Function<StreamObserver<ResT>, StreamObserver<ReqT>> rpcFunc) {
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
    try (LockResource lr = new LockResource(mLock)) {
      while (true) {
        if (mError != null) {
          throw mError;
        }
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
   * @return the response
   * @throws IOException if any error occurs
   */
  public ResT receive(long timeoutMs) throws IOException {
    if (mCompleted) {
      return null;
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
      if (response instanceof IOException) {
        throw (IOException) response;
      }
      return (ResT) response;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CanceledException(e);
    }
  }

  /**
   * Closes the stream.
   */
  public void close() {
    if (!mCanceled && !mClosed) {
      mClosed = true;
      mRequestObserver.onCompleted();
    }
  }

  /**
   * Cancels the stream.
   */
  public void cancel() {
    if (!mCanceled && !mClosed) {
      mCanceled = true;
      mRequestObserver.cancel("Request is cancelled by user.", null);
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

  private final class ResponseStreamObserver
      implements ClientResponseObserver<ReqT, ResT> {

    @Override
    public void onNext(ResT response) {
      try {
        mResponses.put(response);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onError(Throwable t) {
      try (LockResource lr = new LockResource(mLock)) {
        if (t instanceof StatusRuntimeException
            && ((StatusRuntimeException) t).getStatus().getCode() == Status.Code.CANCELLED) {
          mResponses.put(this);
        } else {
          updateException(t);
          mReadyOrFailed.signal();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onCompleted() {
      try (LockResource lr = new LockResource(mLock)) {
        mResponses.put(this);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
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

    /**
     * Updates the channel exception to be the given exception e, or adds e to
     * suppressed exceptions.
     *
     * @param e Exception received
     */
    @GuardedBy("mLock")
    private void updateException(Throwable e) {
      AlluxioStatusException ex;
      if (e instanceof StatusRuntimeException) {
        ex = GrpcExceptionUtils.fromGrpcStatusException((StatusRuntimeException) e);
      } else {
        ex = AlluxioStatusException.fromThrowable(mError);
      }
      if (mError == null || mError == e) {
        mError = ex;
        try {
          mResponses.put(AlluxioStatusException.fromCheckedException(mError));
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(
              String.format("Interrupted while processing error %s", e.getMessage()));
        }
      } else {
        mError.addSuppressed(e);
      }
    }
  }
}
