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

import alluxio.client.file.options.InStreamOptions;
import alluxio.grpc.ReadPType;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NioDataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.BlockReadRequest;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A data reader that reads from a worker in the same process of this client directly.
 *
 * This data reader is similar to read from local worker via {@link GrpcDataReader}
 * except that all communication with the local worker is via internal method call
 * instead of external RPC frameworks.
 */
@NotThreadSafe
public final class BlockWorkerDataReader implements DataReader {
  private static final Logger LOG = LoggerFactory.getLogger(BlockWorkerDataReader.class);

  /** The block reader to read from the local worker block or UFS block. */
  private final BlockReader mReader;
  private final long mEnd;
  private final long mChunkSize;
  private long mPos;
  private boolean mClosed;
  private final ExecutorService mExecutorService;

  /**
   * Creates an instance of {@link BlockWorkerDataReader}.
   *
   * @param executor the thread pool executor that processes reading request
   * @param reader the block reader to read data from
   * @param offset the offset
   * @param len the length to read
   * @param chunkSize the chunk size
   */
  private BlockWorkerDataReader(ExecutorService executor, BlockReader reader,
      long offset, long len, long chunkSize) {
    mReader = reader;
    Preconditions.checkArgument(chunkSize > 0);
    mPos = offset;
    mEnd = Math.min(mReader.getLength(), offset + len);
    mChunkSize = chunkSize;
    mExecutorService = executor;
  }

  @Override
  public DataBuffer readChunk() throws IOException {
    if (mPos >= mEnd) {
      return null;
    }
    Callable<ByteBuffer> readTask = () -> mReader.read(mPos, Math.min(mChunkSize, mEnd - mPos));
    Future<ByteBuffer> readResult = mExecutorService.submit(readTask);
    ByteBuffer buffer = null;
    try {
      buffer = readResult.get();
    } catch (Exception e) {
      LOG.error("Failed to read with Fuse in worker.", e);
      throw new IOException("Failed to read with Fuse in worker.");
    }
    DataBuffer dataBuffer = new NioDataBuffer(buffer, buffer.remaining());
    mPos += dataBuffer.getLength();
    MetricsSystem.counter(MetricKey.WORKER_BYTES_READ_DIRECT.getName()).inc(dataBuffer.getLength());
    MetricsSystem.meter(MetricKey.WORKER_BYTES_READ_DIRECT_THROUGHPUT.getName())
        .mark(dataBuffer.getLength());
    return dataBuffer;
  }

  @Override
  public long pos() {
    return mPos;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    if (mReader != null) {
      mReader.close();
    }
    mClosed = true;
  }

  /**
   * Factory class to create {@link BlockWorkerDataReader}s.
   */
  @NotThreadSafe
  public static class Factory implements DataReader.Factory {
    private final long mChunkSize;
    private final BlockWorker mBlockWorker;
    private final long mBlockId;
    private final boolean mIsPromote;
    private final boolean mIsPositionShort;
    private final Protocol.OpenUfsBlockOptions mOpenUfsBlockOptions;
    private BlockReadRequest mBlockReadRequest;

    /**
     * Creates an instance of {@link Factory}.
     *
     * @param blockWorker the block worker
     * @param blockId the block ID
     * @param chunkSize chunk size in bytes
     * @param options the instream options
     */
    public Factory(BlockWorker blockWorker, long blockId,
        long chunkSize, InStreamOptions options)  {
      Preconditions.checkNotNull(blockWorker);
      mBlockId = blockId;
      mBlockWorker = blockWorker;
      mChunkSize = chunkSize;
      mIsPromote = options.getOptions().getReadType() == ReadPType.CACHE_PROMOTE;
      mIsPositionShort = options.getPositionShort();
      mOpenUfsBlockOptions = options.getOpenUfsBlockOptions(blockId);
    }

    @Override
    public DataReader create(long offset, long len) throws IOException {
      mBlockReadRequest = new BlockReadRequest(mBlockId, offset, offset + len, mChunkSize,
          mIsPromote, mIsPositionShort, mOpenUfsBlockOptions);
      try {
        BlockReader reader = mBlockWorker.createBlockReader(mBlockReadRequest);
        ExecutorService executorService = mBlockWorker.getWorkerFuseExecutorService("read");
        return new BlockWorkerDataReader(executorService, reader, offset, len, mChunkSize);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close() throws IOException {}
  }
}

