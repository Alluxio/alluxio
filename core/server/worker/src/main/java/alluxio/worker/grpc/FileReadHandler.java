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

import static alluxio.util.CommonUtils.isFatalError;

import alluxio.ProcessUtils;
import alluxio.RpcSensitiveConfigMask;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.Chunk;
import alluxio.grpc.DataMessage;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NettyDataBuffer;
import alluxio.network.protocol.databuffer.PooledDirectNioByteBuf;
import alluxio.resource.LockResource;
import alluxio.util.LogUtils;
import alluxio.wire.BlockReadRequest;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.dora.DoraWorker;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.internal.SerializingExecutor;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * Handles file read request.
 */
public class FileReadHandler implements StreamObserver<ReadRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockReadHandler.class);

  private static final long MAX_CHUNK_SIZE =
      Configuration.getBytes(PropertyKey.WORKER_NETWORK_READER_MAX_CHUNK_SIZE_BYTES);

  private static final long MAX_BYTES_IN_FLIGHT =
      Configuration.getBytes(PropertyKey.WORKER_NETWORK_READER_BUFFER_SIZE_BYTES);

  private final ExecutorService mDataReaderExecutor;
  private final StreamObserver<ReadResponse> mResponseObserver;
  private final SerializingExecutor mSerializingExecutor;
  private final DoraWorker mWorker;
  private final boolean mIsReaderBufferPooled;

  private final ReentrantLock mLock = new ReentrantLock();

  private static final Counter RPC_READ_COUNT =
      MetricsSystem.counterWithTags(MetricKey.WORKER_ACTIVE_RPC_READ_COUNT.getName(),
          MetricKey.WORKER_ACTIVE_RPC_READ_COUNT.isClusterAggregated());

  /**
   * This is only created in the gRPC event thread when a read request is received.
   * Using "volatile" because we want any value change of this variable to be
   * visible across both gRPC and I/O threads, meanwhile no atomicity of operation is assumed;
   */
  private volatile BlockReadRequestContext mContext;

  /**
   * Creates an instance of {@link FileReadHandler}.
   *
   * @param executorService the executor service to run data readers
   * @param worker block worker
   * @param responseObserver the response observer of the
   */
  FileReadHandler(ExecutorService executorService,
      DoraWorker worker,
      StreamObserver<ReadResponse> responseObserver) {
    mDataReaderExecutor = executorService;
    mResponseObserver = responseObserver;
    mSerializingExecutor =
        new SerializingExecutor(GrpcExecutors.BLOCK_READER_SERIALIZED_RUNNER_EXECUTOR);
    mWorker = worker;
    mIsReaderBufferPooled =
        Configuration.getBoolean(PropertyKey.WORKER_NETWORK_READER_BUFFER_POOLED);
  }

  @Override
  public void onNext(alluxio.grpc.ReadRequest request) {
    // Expected state: context equals null as this handler is new for request.
    // Otherwise, notify the client an illegal state. Note that, we reset the context before
    // validation msg as validation may require to update error in context.
    LOG.debug("Received read request {}.",
        RpcSensitiveConfigMask.CREDENTIAL_FIELD_MASKER.maskObjects(LOG, request));
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
      mContext.setPosToQueue(mContext.getRequest().getStart());
      mContext.setPosReceived(mContext.getRequest().getStart());
      mDataReaderExecutor.submit(new DataReader(mContext, mResponseObserver));
      mContext.setDataReaderActive(true);
    } catch (RejectedExecutionException e) {
      handleStreamEndingException(Status.RESOURCE_EXHAUSTED.withCause(e)
          .withDescription("Failed to create a new data reader"));
    } catch (Exception e) {
      handleStreamEndingException(
          AlluxioStatusException.fromThrowable(e).toGrpcStatusException().getStatus());
    }
  }

  @Override
  public void onError(Throwable cause) {
    BlockReadRequest r = mContext == null ? null : mContext.getRequest();
    LogUtils.warnWithException(LOG, "Exception occurred while processing read request onError "
            + "sessionId: {}, {}",
        r, r == null ? null : r.getSessionId(), cause);
    setError(new Error(AlluxioStatusException.fromThrowable(cause), false));
  }

  @Override
  public void onCompleted() {
    try (LockResource lr = new LockResource(mLock)) {
      if (mContext == null || mContext.getError() != null || mContext.isEof()
          || mContext.isCancel()) {
        return;
      }
      mContext.setCancel(true);
      if (!mContext.isDataReaderActive()) {
        mContext.setDataReaderActive(true);
        new DataReader(mContext, mResponseObserver).run();
      }
    }
  }

  /**
   * @return true if there are too many chunks in-flight
   */
  @GuardedBy("mLock")
  public boolean tooManyPendingChunks() {
    return mContext.getPosToQueue() - mContext.getPosReceived() >= MAX_BYTES_IN_FLIGHT;
  }

  /**
   * Ready to restart data reader.
   */
  public void onReady() {
    try (LockResource lr = new LockResource(mLock)) {
      if (shouldRestartDataReader()) {
        try {
          mDataReaderExecutor.submit(new DataReader(mContext, mResponseObserver));
          mContext.setDataReaderActive(true);
        } catch (RejectedExecutionException e) {
          handleStreamEndingException(Status.RESOURCE_EXHAUSTED.withCause(e)
              .withDescription("Failed to create a new data reader"));
        }
      }
    }
  }

  /**
   * Handles any exception which should abort the client's read request.
   *
   * @param status the type of {@link Status} exception which should be returned to the user
   */
  private void handleStreamEndingException(Status status) {
    Long sessionId = mContext.getRequest() == null ? -1 : mContext.getRequest().getSessionId();
    LogUtils.warnWithException(LOG, "Error occurred while handling read. sessionId: {}. Ending "
            + "stream",
        sessionId, status);
    AlluxioStatusException statusExc = AlluxioStatusException.from(status);
    try (LockResource lr = new LockResource(mLock)) {
      if (mContext == null) {
        mContext = createRequestContext(alluxio.grpc.ReadRequest.newBuilder().build());
      }
      setError(new Error(statusExc, true));
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
        new DataReader(mContext, mResponseObserver).run();
      }
    }
  }

  /**
   * @param request the block read request
   * @return an instance of read request based on the request read from channel
   */
  protected BlockReadRequestContext createRequestContext(alluxio.grpc.ReadRequest request) {
    BlockReadRequestContext context = new BlockReadRequestContext(request);

    context.setCounter(MetricsSystem.counter(MetricKey.WORKER_BYTES_READ_REMOTE.getName()));
    context.setMeter(MetricsSystem
        .meter(MetricKey.WORKER_BYTES_READ_REMOTE_THROUGHPUT.getName()));
    RPC_READ_COUNT.inc();
    return context;
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
  private class DataReader implements Runnable {
    private final CallStreamObserver<ReadResponse> mResponse;
    private final BlockReadRequestContext mContext;
    private final BlockReadRequest mRequest;
    private final long mChunkSize;

    /**
     * Creates an instance of the {@link FileReadHandler.DataReader}.
     *
     * @param context context of the request to complete
     * @param response the response
     */
    DataReader(BlockReadRequestContext context, StreamObserver<ReadResponse> response) {
      mContext = Preconditions.checkNotNull(context);
      mRequest = Preconditions.checkNotNull(context.getRequest());
      mChunkSize = Math.min(mRequest.getChunkSize(), MAX_CHUNK_SIZE);
      mResponse = (CallStreamObserver<ReadResponse>) response;
    }

    @Override
    public void run() {
      try {
        runInternal();
      } catch (Throwable e) {
        throw e;
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
          new DataReader(mContext, mResponseObserver).run();
        }
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
          if (mContext.isDoneUnsafe()) {
            return;
          }
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

        DataBuffer chunk;
        try {
          // Once we get the data buffer, the lock on the block has been acquired.
          // If there are any stream errors during this time, we must unlock the block
          // before exiting.
          chunk = getDataBuffer(mContext, start, chunkSize);
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
                finalChunk.release();
              }
            });
          }
        } catch (Throwable e) {
          if (isFatalError(e)) {
            ProcessUtils.fatalError(LOG, e, "Error while reading");
          }
          LogUtils.warnWithException(LOG,
              "Exception occurred while reading data for read request {}. session {}",
              mContext.getRequest(), mContext.getRequest().getSessionId(),
              e);
          setError(new Error(AlluxioStatusException.fromThrowable(e), true));
        }
        continue;
      }
      if (error != null) {
        try {
          completeRequest(mContext);
        } catch (Exception e) {
          LOG.error("Failed to close the request.", e);
        }
        replyError(error);
      } else if (eof || cancel) {
        try {
          completeRequest(mContext);
        } catch (Exception e) {
          LogUtils.warnWithException(LOG, "Exception occurred while completing read request, "
                  + "EOF/CANCEL sessionId: {}. {}", mContext.getRequest().getSessionId(),
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
     * Opens the block if it is not open.
     *
     * @throws Exception if it fails to open the block
     */
    private void openBlock(BlockReadRequestContext context)
        throws Exception {
      if (context.getBlockReader() != null) {
        return;
      }
      BlockReadRequest request = context.getRequest();
      BlockReader reader = mWorker.createFileReader(request.getOpenUfsBlockOptions().getUfsPath(),
          request.getStart(), request.isPositionShort(), request.getOpenUfsBlockOptions());
      context.setBlockReader(reader);
    }

    /**
     * Completes the read request. When the request is closed, we should clean up any temporary
     * state it may have accumulated.
     *
     * @param context context of the request to complete
     */
    private void completeRequest(BlockReadRequestContext context) throws Exception {
      BlockReader reader = context.getBlockReader();
      try {
        if (reader != null) {
          reader.close();
        }
      } finally {
        context.setBlockReader(null);
        RPC_READ_COUNT.dec();
      }
    }

    /**
     * Returns the appropriate {@link DataBuffer} representing the data to send, depending on the
     * configurable transfer type.
     *
     * @param context context of the request to complete
     * @param len The length, in bytes, of the data to read from the block
     * @return a {@link DataBuffer} representing the data
     */
    protected DataBuffer getDataBuffer(BlockReadRequestContext context, long offset, int len)
        throws Exception {
      @Nullable
      BlockReader blockReader = null;
      // timings
      long openMs = -1;
      long startTransferMs = -1;
      long startMs = System.currentTimeMillis();

      openBlock(context);
      openMs = System.currentTimeMillis() - startMs;
      blockReader = context.getBlockReader();
      Preconditions.checkState(blockReader != null);
      startTransferMs = System.currentTimeMillis();
      ByteBuf buf;
      if (mIsReaderBufferPooled) {
        buf = PooledDirectNioByteBuf.allocate(len);
      } else {
        buf = Unpooled.directBuffer(len, len);
      }
      try {
        while (buf.writableBytes() > 0 && blockReader.transferTo(buf) != -1) {
        }
        return new NettyDataBuffer(buf.retain());
      } finally {
        buf.release();
      }
    }

    /**
     * Writes an error read response to the channel and closes the channel after that.
     */
    private void replyError(Error error) {
      mSerializingExecutor.execute(() -> {
        try {
          if (!mContext.isDoneUnsafe()) {
            mResponse.onError(error.getCause().toGrpcStatusException());
            mContext.setDoneUnsafe(true);
          } else  {
            LOG.debug("Tried to replyError when stream was already completed. context: {}",
                mContext);
          }
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
          if (!mContext.isDoneUnsafe()) {
            mContext.setDoneUnsafe(true);
            mResponse.onCompleted();
          } else {
            LOG.debug("Tried to replyEof when stream was already finished. context: {}", mContext);
          }
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
          if (!mContext.isDoneUnsafe()) {
            mContext.setDoneUnsafe(true);
            mResponse.onCompleted();
          } else {
            LOG.debug("Tried to replyCancel when stream was already finished. context: {}",
                mContext);
          }
        } catch (StatusRuntimeException e) {
          if (e.getStatus().getCode() != Status.Code.CANCELLED) {
            throw e;
          }
        }
      });
    }
  }
}
