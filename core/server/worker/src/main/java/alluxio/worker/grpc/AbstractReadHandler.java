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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.Chunk;
import alluxio.grpc.DataMessage;
import alluxio.grpc.ReadResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.resource.LockResource;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.util.LogUtils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.internal.SerializingExecutor;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class handles {@link ReadRequest}s.
 *
 * Protocol: Check {@link alluxio.client.block.stream.GrpcDataReader} for additional information.
 * 1. Once a read request is received, the handler creates a {@link DataReader} which reads
 *    chunks of data from the block worker and pushes them to the buffer.
 * 2. The {@link DataReader} pauses if there are too many packets in flight, and resumes if there
 *    is room available.
 * 3. The channel is closed if there is any exception during the data read/write.
 *
 * Threading model:
 * Only two threads are involved at a given point of time: gRPC event thread, data reader thread.
 * 1. The gRPC event thread accepts the read request, handles write callbacks. If any exception
 *    occurs (e.g. failed to read from stream or respond to stream) or the read request is cancelled
 *    by the client, the gRPC event thread notifies the data reader thread.
 * 2. The data reader thread keeps reading from the file and writes to buffer. Before reading a
 *    new data chunk, it checks whether there are notifications (e.g. cancel, error), if
 *    there is, handle them properly. See more information about the notifications in the javadoc
 *    of {@link ReadRequestContext#mCancel#mEof#mError}.
 *
 * @param <T> type of read request
 */
@NotThreadSafe
abstract class AbstractReadHandler<T extends ReadRequestContext<?>>
    implements StreamObserver<alluxio.grpc.ReadRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractReadHandler.class);
  private static final long MAX_CHUNK_SIZE =
      ServerConfiguration.getBytes(PropertyKey.WORKER_NETWORK_READER_MAX_CHUNK_SIZE_BYTES);
  private static final long MAX_BYTES_IN_FLIGHT =
      ServerConfiguration.getBytes(PropertyKey.WORKER_NETWORK_READER_BUFFER_SIZE_BYTES);

  /** The executor to run {@link DataReader}. */
  private final ExecutorService mDataReaderExecutor;
  /** A serializing executor for sending responses. */
  private Executor mSerializingExecutor;

  private final ReentrantLock mLock = new ReentrantLock();

  protected AuthenticatedUserInfo mUserInfo;

  /**
   * This is only created in the gRPC event thread when a read request is received.
   * Using "volatile" because we want any value change of this variable to be
   * visible across both gRPC and I/O threads, meanwhile no atomicity of operation is assumed;
   */
  private volatile T mContext;
  private StreamObserver<ReadResponse> mResponseObserver;

  /**
   * Creates an instance of {@link AbstractReadHandler}.
   *
   * @param executorService the executor service to run {@link DataReader}s
   * @param responseObserver the response observer of the
   * @param userInfo the authenticated user info
   */
  AbstractReadHandler(ExecutorService executorService,
      StreamObserver<ReadResponse> responseObserver, AuthenticatedUserInfo userInfo) {
    mDataReaderExecutor = executorService;
    mSerializingExecutor = new SerializingExecutor(executorService);
    mResponseObserver = responseObserver;
    mUserInfo = userInfo;
  }

  @Override
  public void onNext(alluxio.grpc.ReadRequest request) {
    // Expected state: context equals null as this handler is new for request.
    // Otherwise, notify the client an illegal state. Note that, we reset the context before
    // validation msg as validation may require to update error in context.
    LOG.debug("Received read request {}.", request);
    try (LockResource lr = new LockResource(mLock)) {
      if (request.hasOffsetReceived()) {
        mContext.setPosReceived(request.getOffsetReceived());
        if (!tooManyPendingChunks()) {
          onReady();
        }
        return;
      }
      Preconditions.checkState(mContext == null || !mContext.isDataReaderActive());
      mContext = createRequestContext(request);
      validateReadRequest(request);
      mContext.setPosToQueue(mContext.getRequest().getStart());
      mContext.setPosReceived(mContext.getRequest().getStart());
      mDataReaderExecutor.submit(createDataReader(mContext, mResponseObserver));
      mContext.setDataReaderActive(true);
    } catch (Exception e) {
      LogUtils.warnWithException(LOG, "Exception occurred while processing read request {}.",
          request, e);
      mSerializingExecutor.execute(() -> mResponseObserver
          .onError(AlluxioStatusException.fromCheckedException(e).toGrpcStatusException()));
    }
  }

  /**
   * @return true if there are too many chunks in-flight
   */
  @GuardedBy("mLock")
  public boolean tooManyPendingChunks() {
    return mContext.getPosToQueue() - mContext.getPosReceived() >= MAX_BYTES_IN_FLIGHT;
  }

  @Override
  public void onError(Throwable cause) {
    LogUtils.warnWithException(LOG, "Exception occurred while processing read request {}",
        mContext == null ? null : mContext.getRequest(), cause);
    setError(new Error(AlluxioStatusException.fromThrowable(cause), false));
  }

  @Override
  public void onCompleted() {
    setCancel();
  }

  /**
   * Validates a read request.
   *
   * @param request the block read request
   * @throws InvalidArgumentException if the request is invalid
   */
  private void validateReadRequest(alluxio.grpc.ReadRequest request)
      throws InvalidArgumentException {
    if (request.getBlockId() < 0) {
      throw new InvalidArgumentException(
          String.format("Invalid blockId (%d) in read request.", request.getBlockId()));
    }
    if (request.getOffset() < 0 || request.getLength() <= 0) {
      throw new InvalidArgumentException(
          String.format("Invalid read bounds in read request %s.", request.toString()));
    }
  }

  /**
   * @param error the error
   */
  private void setError(Error error) {
    Preconditions.checkNotNull(error, "error");
    try (LockResource lr = new LockResource(mLock)) {
      if (mContext == null || mContext.getError() != null || mContext.isDoneUnsafe()) {
        // Note, we may reach here via channelUnregistered due to network errors bubbling up before
        // mContext is initialized, or channel garbage collection after the request is finished.
        return;
      }
      mContext.setError(error);
      if (!mContext.isDataReaderActive()) {
        mContext.setDataReaderActive(true);
        mDataReaderExecutor.submit(createDataReader(mContext, mResponseObserver));
      }
    }
  }

  private void setEof() {
    try (LockResource lr = new LockResource(mLock)) {
      if (mContext == null || mContext.getError() != null || mContext.isCancel()
          || mContext.isEof()) {
        return;
      }
      mContext.setEof(true);
      if (!mContext.isDataReaderActive()) {
        mContext.setDataReaderActive(true);
        mDataReaderExecutor.submit(createDataReader(mContext, mResponseObserver));
      }
    }
  }

  private void setCancel() {
    try (LockResource lr = new LockResource(mLock)) {
      if (mContext == null || mContext.getError() != null || mContext.isEof()
          || mContext.isCancel()) {
        return;
      }
      mContext.setCancel(true);
      if (!mContext.isDataReaderActive()) {
        mContext.setDataReaderActive(true);
        mDataReaderExecutor.submit(createDataReader(mContext, mResponseObserver));
      }
    }
  }

  /**
   * @param request the block read request
   * @return an instance of read request based on the request read from channel
   */
  protected abstract T createRequestContext(alluxio.grpc.ReadRequest request);

  /**
   * Creates a read reader.
   *
   * @param context read request context
   * @param channel channel
   * @return the data reader for this handler
   */
  protected abstract DataReader createDataReader(T context,
      StreamObserver<ReadResponse> channel);

  public void onReady() {
    try (LockResource lr = new LockResource(mLock)) {
      if (shouldRestartDataReader()) {
        mDataReaderExecutor.submit(createDataReader(mContext, mResponseObserver));
        mContext.setDataReaderActive(true);
      }
    }
  }

  /**
   * @return true if we should restart the data reader
   */
  @GuardedBy("mLock")
  private boolean shouldRestartDataReader() {
    return mContext != null && !mContext.isDataReaderActive()
        && mContext.getPosToQueue() < mContext.getRequest().getEnd()
        && mContext.getError() == null && !mContext.isCancel() && !mContext.isEof();
  }

  /**
   * @param bytesRead bytes read
   */
  private void incrementMetrics(long bytesRead) {
    Counter counter = mContext.getCounter();
    Meter meter = mContext.getMeter();
    Preconditions.checkState(counter != null);
    counter.inc(bytesRead);
    meter.mark(bytesRead);
  }

  /**
   * A runnable that reads data and writes them to the channel.
   */
  protected abstract class DataReader implements Runnable {
    private final CallStreamObserver<ReadResponse> mResponse;
    private final T mContext;
    private final ReadRequest mRequest;
    private final long mChunkSize;

    /**
     * Creates an instance of the {@link DataReader}.
     *
     * @param context context of the request to complete
     * @param response the response
     */
    DataReader(T context, StreamObserver<ReadResponse> response) {
      mContext = context;
      mRequest = context.getRequest();
      mChunkSize = Math.min(mRequest.getChunkSize(), MAX_CHUNK_SIZE);
      mResponse = (CallStreamObserver<ReadResponse>) response;
    }

    @Override
    public void run() {
      try {
        runInternal();
      } catch (Throwable e) {
        LOG.error("Failed to run DataReader.", e);
        throw new RuntimeException(e);
      }
    }

    private void runInternal() {
      boolean eof;  // End of file. Everything requested has been read.
      boolean cancel;
      Error error;  // error occurred, abort requested.
      while (true) {
        final long start;
        final int chunkSize;
        try (LockResource lr = new LockResource(mLock)) {
          start = mContext.getPosToQueue();
          eof = mContext.isEof();
          cancel = mContext.isCancel();
          error = mContext.getError();

          if (eof || cancel || error != null || (!mResponse.isReady() && tooManyPendingChunks())) {
            mContext.setDataReaderActive(false);
            break;
          }
          chunkSize = (int) Math.min(mRequest.getEnd() - mContext.getPosToQueue(), mChunkSize);

          // chunkSize should always be > 0 here when reaches here.
          Preconditions.checkState(chunkSize > 0);
        }

        DataBuffer chunk = null;
        try {
          chunk = getDataBuffer(mContext, mResponse, start, chunkSize);
          if (chunk != null) {
            try (LockResource lr = new LockResource(mLock)) {
              mContext.setPosToQueue(mContext.getPosToQueue() + chunk.getLength());
            }
          }
          if (chunk == null || chunk.getLength() < chunkSize || start + chunkSize == mRequest
              .getEnd()) {
            // This can happen if the requested read length is greater than the actual length of the
            // block or file starting from the given offset.
            setEof();
          }

          if (chunk != null) {
            DataBuffer finalChunk = chunk;
            mSerializingExecutor.execute(() -> {
              try {
                ReadResponse response = ReadResponse.newBuilder().setChunk(Chunk.newBuilder()
                    .setData(UnsafeByteOperations.unsafeWrap(finalChunk.getReadOnlyByteBuffer()))
                ).build();
                if (mResponse instanceof DataMessageServerStreamObserver) {
                  ((DataMessageServerStreamObserver<ReadResponse>) mResponse)
                      .onNext(new DataMessage<>(response, finalChunk));
                } else {
                  mResponse.onNext(response);
                }
                incrementMetrics(finalChunk.getLength());
              } catch (Exception e) {
                LogUtils.warnWithException(LOG,
                    "Exception occurred while sending data for read request {}.",
                    mContext.getRequest(), e);
                setError(new Error(AlluxioStatusException.fromThrowable(e), true));
              } finally {
                if (finalChunk != null) {
                  finalChunk.release();
                }
              }
            });
          }
        } catch (Exception e) {
          LogUtils.warnWithException(LOG,
              "Exception occurred while reading data for read request {}.", mContext.getRequest(),
              e);
          setError(new Error(AlluxioStatusException.fromThrowable(e), true));
          continue;
        }
      }

      if (error != null) {
        try {
          // mRequest is null if an exception is thrown when initializing mRequest.
          if (mRequest != null) {
            completeRequest(mContext);
          }
        } catch (Exception e) {
          LOG.error("Failed to close the request.", e);
        }
        replyError(error);
      } else if (eof || cancel) {
        try {
          completeRequest(mContext);
        } catch (Exception e) {
          LogUtils.warnWithException(LOG, "Exception occurred while completing read request {}.",
              mContext.getRequest(), e);
          setError(new Error(AlluxioStatusException.fromThrowable(e), true));
        }
        if (eof) {
          replyEof();
        } else {
          replyCancel();
        }
      }
    }

    /**
     * Completes the read request. When the request is closed, we should clean up any temporary
     * state it may have accumulated.
     *
     * @param context context of the request to complete
     */
    protected abstract void completeRequest(T context) throws Exception;

    /**
     * Returns the appropriate {@link DataBuffer} representing the data to send, depending on the
     * configurable transfer type.
     *
     * @param context context of the request to complete
     * @param response the gRPC response observer
     * @param len The length, in bytes, of the data to read from the block
     * @return a {@link DataBuffer} representing the data
     */
    protected abstract DataBuffer getDataBuffer(T context, StreamObserver<ReadResponse> response,
        long offset, int len) throws Exception;

    /**
     * Writes an error read response to the channel and closes the channel after that.
     */
    private void replyError(Error error) {
      mSerializingExecutor.execute(() -> {
        try {
          mResponse.onError(error.getCause().toGrpcStatusException());
        } catch (StatusRuntimeException e) {
          // Ignores the error when client already closed the stream.
          if (e.getStatus().getCode() != Status.Code.CANCELLED) {
            throw e;
          }
        }
      });
    }

    /**
     * Writes a success response.
     */
    private void replyEof() {
      mSerializingExecutor.execute(() -> {
        try {
          Preconditions.checkState(!mContext.isDoneUnsafe());
          mContext.setDoneUnsafe(true);
          mResponse.onCompleted();
        } catch (StatusRuntimeException e) {
          if (e.getStatus().getCode() != Status.Code.CANCELLED) {
            throw e;
          }
        }
      });
    }

    /**
     * Writes a cancel response.
     */
    private void replyCancel() {
      mSerializingExecutor.execute(() -> {
        try {
          Preconditions.checkState(!mContext.isDoneUnsafe());
          mContext.setDoneUnsafe(true);
          mResponse.onCompleted();
        } catch (StatusRuntimeException e) {
          if (e.getStatus().getCode() != Status.Code.CANCELLED) {
            throw e;
          }
        }
      });
    }
  }
}
