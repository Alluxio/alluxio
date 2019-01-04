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

package alluxio.worker.grpc;

import alluxio.client.block.stream.GrpcDataWriter;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteRequestCommand;
import alluxio.grpc.WriteResponse;
import alluxio.resource.LockResource;

import com.google.protobuf.ByteString;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class handles {@link WriteRequest}s.
 *
 * Protocol: Check {@link GrpcDataWriter} for more information.
 * 1. The write handler streams data from the client and writes them. The flow control is handled
 *    by gRPC server.
 * 3. A complete or cancel event signifies the completion of this request.
 * 4. When an error occurs, the response stream is closed.
 *
 * Threading model:
 * The write handler runs the gRPC event threads. gRPC serializes the events so there will not be
 * events running concurrently for same request.
 *
 * @param <T> type of write request
 */
@NotThreadSafe
abstract class AbstractWriteHandler<T extends WriteRequestContext<?>> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractWriteHandler.class);

  private final StreamObserver<WriteResponse> mResponseObserver;

  private ReentrantLock mLock = new ReentrantLock();

  /**
   * This is initialized only once for a whole file or block in
   * {@link AbstractWriteHandler#write(WriteRequest)}.
   * It is safe to read those final primitive fields (e.g. mId, mSessionId) if mError is not set.
   *
   * Using "volatile" because we want any value change of this variable to be
   * visible across all gRPC threads.
   */
  private volatile T mContext;

  /**
   * Creates an instance of {@link AbstractWriteHandler}.
   *
   * @param responseObserver the response observer for the write request
   */
  AbstractWriteHandler(StreamObserver<WriteResponse> responseObserver) {
    mResponseObserver = responseObserver;
  }

  /**
   * Handles write request.
   *
   * @param writeRequest the request from the client
   */
  public void write(WriteRequest writeRequest) {
    try (LockResource lr = new LockResource(mLock)) {
      if (mContext == null) {
        mContext = createRequestContext(writeRequest);
      } else {
        Preconditions.checkState(!mContext.isDoneUnsafe(),
            "invalid request after write request is completed.");
      }
      validateWriteRequest(writeRequest);
      if (writeRequest.hasCommand()) {
        WriteRequestCommand command = writeRequest.getCommand();
        if (command.getFlush()) {
          flush();
        } else {
          handleCommand(command, mContext);
        }
      } else {
        Preconditions.checkState(writeRequest.hasChunk(),
            "write request is missing data chunk in non-command message");
        ByteString data = writeRequest.getChunk().getData();
        Preconditions.checkState(data != null && data.size() > 0,
            "invalid data size from write request message");
        writeData(data);
      }
    } catch (Exception e) {
      abort(new Error(AlluxioStatusException.fromThrowable(e), true));
    }
  }

  /**
   * Handles request complete event.
   */
  public void onCompleted() {
    Preconditions.checkState(mContext != null);
    try (LockResource lr = new LockResource(mLock)) {
      completeRequest(mContext);
      replySuccess();
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      abort(new Error(AlluxioStatusException.fromCheckedException(e), true));
    }
  }

  /**
   * Handles request cancellation event.
   */
  public void onCancel() {
    try (LockResource lr = new LockResource(mLock)) {
      cancelRequest(mContext);
      replyCancel();
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      abort(new Error(AlluxioStatusException.fromCheckedException(e), true));
    }
  }

  /**
   * Handles errors from the request.
   *
   * @param cause the exception
   */
  public void onError(Throwable cause) {
    LOG.error("Exception thrown while handling write request {}:",
        mContext == null ? "unknown" : mContext.getRequest(), cause);
    abort(new Error(AlluxioStatusException.fromThrowable(cause), false));
  }

  /**
   * Validates a block write request.
   *
   * @param request the block write request
   * @throws InvalidArgumentException if the write request is invalid
   */
  @GuardedBy("mLock")
  private void validateWriteRequest(alluxio.grpc.WriteRequest request)
      throws InvalidArgumentException {
    if (request.hasCommand() && request.getCommand().hasOffset()
        && request.getCommand().getOffset() != mContext.getPos()) {
      throw new InvalidArgumentException(String.format(
          "Offsets do not match [received: %d, expected: %d].",
          request.getCommand().getOffset(), mContext.getPos()));
    }
  }

  private void writeData(ByteString buf) {
    try {
      int readableBytes = buf.size();
      mContext.setPos(mContext.getPos() + readableBytes);
      writeBuf(mContext, mResponseObserver, buf, mContext.getPos());
      incrementMetrics(readableBytes);
    } catch (Exception e) {
      LOG.error("Failed to write data for request {}", mContext.getRequest(), e);
      Throwables.throwIfUnchecked(e);
      abort(new Error(AlluxioStatusException.fromCheckedException(e), true));
    }
  }

  private void flush() {
    try (LockResource lr = new LockResource(mLock)) {
      flushRequest(mContext);
      replyFlush();
    } catch (Exception e) {
      LOG.error("Failed to flush for write request {}", mContext.getRequest(), e);
      Throwables.throwIfUnchecked(e);
      abort(new Error(AlluxioStatusException.fromCheckedException(e), true));
    }
  }

  /**
   * Abort the write process due to error.
   *
   * @param error the error
   */
  private void abort(Error error) {
    try (LockResource lr = new LockResource(mLock)) {
      if (mContext == null || mContext.getError() != null || mContext.isDoneUnsafe()) {
        // Note, we may reach here via events due to network errors bubbling up before
        // mContext is initialized, or stream error after the request is finished.
        return;
      }
      mContext.setError(error);
      cleanupRequest(mContext);
      replyError();
    } catch (Exception e) {
      LOG.warn("Failed to cleanup states with error {}.", e.getMessage());
    }
  }

  /**
   * Creates a new request context.
   *
   * @param msg the block write request
   */
  protected abstract T createRequestContext(WriteRequest msg) throws Exception;

  /**
   * Completes this write. This is called when the write completes.
   *  @param context context of the request to complete
   */
  protected abstract void completeRequest(T context) throws Exception;

  /**
   * Cancels this write. This is called when the client issues a cancel request.
   *
   * @param context context of the request to complete
   */
  protected abstract void cancelRequest(T context) throws Exception;

  /**
   * Cleans up this write. This is called when the write request is aborted due to any exception
   * or session timeout.
   *
   * @param context context of the request to complete
   */
  protected abstract void cleanupRequest(T context) throws Exception;

  /**
   * Flushes the buffered data. Flush only happens after write.
   *
   * @param context context of the request to complete
   */
  protected abstract void flushRequest(T context) throws Exception;

  /**
   * Writes the buffer.
   *  @param context context of the request to complete
   * @param responseObserver the response observer
   * @param buf the buffer
   * @param pos the pos
   */
  protected abstract void writeBuf(T context, StreamObserver<WriteResponse> responseObserver,
      ByteString buf, long pos) throws Exception;

  /**
   * Handles a command in the write request.
   *
   * @param command the command to be handled
   */
  protected void handleCommand(WriteRequestCommand command, T context) throws Exception {
    // no additional command is handled by default
  }

  /**
   * Writes a response to signify the success of the write request.
   */
  private void replySuccess() {
    mContext.setDoneUnsafe(true);
    mResponseObserver.onCompleted();
  }

  /**
   * Writes a response to signify the successful cancellation of the write request.
   */
  private void replyCancel() {
    mContext.setDoneUnsafe(true);
    mResponseObserver.onCompleted();
  }

  /**
   * Writes an error response.
   */
  private void replyError() {
    Error error;
    try (LockResource lr = new LockResource(mLock)) {
      error = Preconditions.checkNotNull(mContext.getError());
    }

    if (error.isNotifyClient()) {
      mResponseObserver.onError(GrpcExceptionUtils.toGrpcStatusException(error.getCause()));
    }
  }

  /**
   * Writes a response to signify the successful flush.
   */
  private void replyFlush() {
    mResponseObserver.onNext(
        WriteResponse.newBuilder().setOffset(mContext.getPos()).build());
  }

  /**
   * @param bytesWritten bytes written
   */
  private void incrementMetrics(long bytesWritten) {
    Counter counter = mContext.getCounter();
    Meter meter = mContext.getMeter();
    Preconditions.checkState(counter != null, "counter");
    Preconditions.checkState(meter != null, "meter");
    counter.inc(bytesWritten);
    meter.mark(bytesWritten);
  }
}
