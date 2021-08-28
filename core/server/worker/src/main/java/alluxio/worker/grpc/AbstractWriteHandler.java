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

import alluxio.Constants;
import alluxio.client.block.stream.GrpcDataWriter;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteRequestCommand;
import alluxio.grpc.WriteResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NioDataBuffer;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.util.LogUtils;
import alluxio.util.logging.SamplingLogger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.internal.SerializingExecutor;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;

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
  private static final Logger SLOW_WRITE_LOG = new SamplingLogger(LOG, 5 * Constants.MINUTE_MS);
  private static final long SLOW_WRITE_MS =
      ServerConfiguration.getMs(PropertyKey.WORKER_REMOTE_IO_SLOW_THRESHOLD);
  public static final long FILE_BUFFER_SIZE = Constants.MB;

  /** The observer for sending response messages. */
  private final StreamObserver<WriteResponse> mResponseObserver;
  /** The executor for running write tasks asynchronously in the submission order. */
  private final SerializingExecutor mSerializingExecutor;
  /** The semaphore to control the number of write tasks queued up in the executor.*/
  private final Semaphore mSemaphore = new Semaphore(
      ServerConfiguration.getInt(PropertyKey.WORKER_NETWORK_WRITER_BUFFER_SIZE_MESSAGES), true);

  /**
   * This is initialized only once for a whole file or block in
   * {@link AbstractWriteHandler#write(WriteRequest)}.
   * It is safe to read those final primitive fields (e.g. mId, mSessionId) if mError is not set.
   *
   * Using "volatile" because we want any value change of this variable to be
   * visible across all gRPC threads.
   */
  private volatile T mContext;

  protected AuthenticatedUserInfo mUserInfo;

  /**
   * Creates an instance of {@link AbstractWriteHandler}.
   *
   * @param responseObserver the response observer for the write request
   * @param userInfo the authenticated user info
   */
  AbstractWriteHandler(StreamObserver<WriteResponse> responseObserver,
      AuthenticatedUserInfo userInfo) {
    mResponseObserver = responseObserver;
    mUserInfo = userInfo;
    mSerializingExecutor = new SerializingExecutor(GrpcExecutors.BLOCK_WRITER_EXECUTOR);
  }

  /**
   * Handles write request.
   *
   * @param writeRequest the request from the client
   */
  public void write(WriteRequest writeRequest) {
    if (!tryAcquireSemaphore()) {
      return;
    }
    mSerializingExecutor.execute(() -> {
      try {
        if (mContext == null) {
          LOG.debug("Received write request {}.", writeRequest);
          try {
            mContext = createRequestContext(writeRequest);
          } catch (Exception e) {
            // abort() assumes context is initialized.
            // Reply with the error in order to prevent clients getting stuck.
            replyError(new Error(AlluxioStatusException.fromThrowable(e), true));
            throw e;
          }
        } else {
          Preconditions.checkState(!mContext.isDoneUnsafe(),
              "invalid request after write request is completed.");
        }
        if (mContext.isDoneUnsafe() || mContext.getError() != null) {
          return;
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
          writeData(new NioDataBuffer(data.asReadOnlyByteBuffer(), data.size()));
        }
      } catch (Exception e) {
        LogUtils.warnWithException(LOG, "Exception occurred while processing write request {}.",
            writeRequest, e);
        abort(new Error(AlluxioStatusException.fromThrowable(e), true));
      } finally {
        mSemaphore.release();
      }
    });
  }

  /**
   * Handles write request with data message.
   *
   * @param request the request from the client
   * @param buffer the data associated with the request
   */
  public void writeDataMessage(WriteRequest request, DataBuffer buffer) {
    if (buffer == null) {
      write(request);
      return;
    }
    boolean releaseBuf = true;
    try {
      Preconditions.checkState(!request.hasCommand(),
          "write request command should not come with data buffer");
      Preconditions.checkState(buffer.readableBytes() > 0,
          "invalid data size from write request message");
      if (!tryAcquireSemaphore()) {
        return;
      }
      releaseBuf = false;
      mSerializingExecutor.execute(() -> {
        try {
          writeData(buffer);
        } finally {
          mSemaphore.release();
        }
      });
    } finally {
      if (releaseBuf) {
        buffer.release();
      }
    }
  }

  /**
   * Handles request complete event.
   */
  public void onCompleted() {
    mSerializingExecutor.execute(() -> {
      Preconditions.checkState(mContext != null);
      try {
        completeRequest(mContext);
        replySuccess();
      } catch (Exception e) {
        LogUtils.warnWithException(LOG, "Exception occurred while completing write request {}.",
            mContext.getRequest(), e);
        abort(new Error(AlluxioStatusException.fromThrowable(e), true));
      }
    });
  }

  /**
   * Handles request cancellation event.
   */
  public void onCancel() {
    mSerializingExecutor.execute(() -> {
      try {
        cancelRequest(mContext);
        replyCancel();
      } catch (Exception e) {
        LogUtils.warnWithException(LOG, "Exception occurred while cancelling write request {}.",
            mContext.getRequest(), e);
        abort(new Error(AlluxioStatusException.fromThrowable(e), true));
      }
    });
  }

  /**
   * Handles errors from the request.
   *
   * @param cause the exception
   */
  public void onError(Throwable cause) {
    if (cause instanceof StatusRuntimeException
        && ((StatusRuntimeException) cause).getStatus().getCode() == Status.Code.CANCELLED) {
      // Cancellation is already handled.
      return;
    }
    mSerializingExecutor.execute(() -> {
      LogUtils.warnWithException(LOG, "Exception thrown while handling write request {}",
          mContext == null ? "unknown" : mContext.getRequest(), cause);
      abort(new Error(AlluxioStatusException.fromThrowable(cause), false));
    });
  }

  private boolean tryAcquireSemaphore() {
    try {
      mSemaphore.acquire();
    } catch (InterruptedException e) {
      LOG.warn("write data request {} is interrupted: {}",
          mContext == null ? "unknown" : mContext.getRequest(), e.getMessage());
      abort(new Error(AlluxioStatusException.fromThrowable(e), true));
      Thread.currentThread().interrupt();
      return false;
    }
    return true;
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

  private void writeData(DataBuffer buf) {
    try {
      if (mContext.isDoneUnsafe() || mContext.getError() != null) {
        return;
      }
      int readableBytes = buf.readableBytes();
      mContext.setPos(mContext.getPos() + readableBytes);

      long writeStartMs = System.currentTimeMillis();
      writeBuf(mContext, mResponseObserver, buf, mContext.getPos());
      long writeMs = System.currentTimeMillis() - writeStartMs;

      if (writeMs >= SLOW_WRITE_MS) {
        // A single write call took much longer than expected.

        String prefix = String
            .format("Writing buffer for remote write took longer than %s ms. handler: %s",
                SLOW_WRITE_MS, this.getClass().getName());

        // Do not template the handler class, so the sampling log can distinguish between
        // different handler types
        SLOW_WRITE_LOG.warn(prefix + " id: {} location: {} bytes: {} durationMs: {}",
            mContext.getRequest().getId(), getLocation(), readableBytes, writeMs);
      }

      incrementMetrics(readableBytes);
    } catch (Exception e) {
      LOG.error("Failed to write data for request {}", mContext.getRequest(), e);
      abort(new Error(AlluxioStatusException.fromThrowable(e), true));
    } finally {
      buf.release();
    }
  }

  private void flush() {
    try {
      flushRequest(mContext);
      replyFlush();
    } catch (Exception e) {
      LOG.error("Failed to flush for write request {}", mContext.getRequest(), e);
      abort(new Error(AlluxioStatusException.fromThrowable(e), true));
    }
  }

  /**
   * Abort the write process due to error.
   *
   * @param error the error
   */
  private void abort(Error error) {
    try {
      if (mContext == null || mContext.getError() != null || mContext.isDoneUnsafe()) {
        // Note, we may reach here via events due to network errors bubbling up before
        // mContext is initialized, or stream error after the request is finished.
        return;
      }
      mContext.setError(error);
      cleanupRequest(mContext);
    } catch (Exception e) {
      LOG.warn("Failed to cleanup states with error {}.", e.toString());
    } finally {
      replyError();
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
   * @param context context of the request to complete
   * @param responseObserver the response observer
   * @param buf the buffer
   * @param pos the pos
   */
  protected abstract void writeBuf(T context, StreamObserver<WriteResponse> responseObserver,
      DataBuffer buf, long pos) throws Exception;

  /**
   * @param context the context of the request
   * @return an informational string of the location the writer is writing to
   */
  protected abstract String getLocationInternal(T context);

  /**
   * @return an informational string of the location the writer is writing to
   */
  public String getLocation() {
    if (mContext == null) {
      return "null";
    }
    return getLocationInternal(mContext);
  }

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
    replyError(Preconditions.checkNotNull(mContext.getError()));
  }

  /**
   * Writes an error response.
   */
  private void replyError(Error error) {
    if (error.isNotifyClient()) {
      mResponseObserver.onError(error.getCause().toGrpcStatusException());
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
