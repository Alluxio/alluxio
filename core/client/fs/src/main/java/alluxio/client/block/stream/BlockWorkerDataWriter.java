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

import alluxio.client.WriteType;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.SessionIdUtils;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockWriter;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A data writer that issues from a client embedded in a worker to write data to the worker directly
 * via internal communication channel.
 *
 * This data writer is similar to write to local worker storage via {@link GrpcDataWriter} except
 * that all communication with the local worker is via internal method call instead of external RPC
 * frameworks.
 */
@NotThreadSafe
public final class BlockWorkerDataWriter implements DataWriter {
  private final long mBlockId;
  private final BlockWriter mBlockWriter;
  private final BlockWorker mBlockWorker;
  private final int mChunkSize;
  private final OutStreamOptions mOptions;
  private final long mSessionId;
  private final long mBufferSize;
  private long mReservedBytes;

  /**
   * Creates an instance of {@link BlockWorkerDataWriter}.
   *
   * @param context the file system context
   * @param blockId the block ID
   * @param blockSize the block size in bytes
   * @param options the output stream options
   * @return the {@link BlockWorkerDataWriter} created
   */
  public static BlockWorkerDataWriter create(final FileSystemContext context, long blockId,
      long blockSize, OutStreamOptions options) throws IOException {
    AlluxioConfiguration conf = context.getClusterConf();
    int chunkSize = (int) conf.getBytes(PropertyKey.USER_LOCAL_WRITER_CHUNK_SIZE_BYTES);
    long reservedBytes = Math.min(blockSize, conf.getBytes(PropertyKey.USER_FILE_RESERVED_BYTES));
    BlockWorker blockWorker = context.getProcessLocalWorker();
    Preconditions.checkNotNull(blockWorker, "blockWorker");
    long sessionId = SessionIdUtils.createSessionId();
    try {
      blockWorker.createBlock(sessionId, blockId, options.getWriteTier(), options.getMediumType(),
          reservedBytes);
      BlockWriter blockWriter = blockWorker.createBlockWriter(sessionId, blockId);
      return new BlockWorkerDataWriter(sessionId, blockId, options, blockWriter, blockWorker,
          chunkSize, reservedBytes, conf);
    } catch (BlockAlreadyExistsException | WorkerOutOfSpaceException | BlockDoesNotExistException
        | InvalidWorkerStateException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long pos() {
    return mBlockWriter.getPosition();
  }

  @Override
  public int chunkSize() {
    return mChunkSize;
  }

  @Override
  public void writeChunk(final ByteBuf buf) throws IOException {
    if (mReservedBytes < pos() + buf.readableBytes()) {
      try {
        long bytesToReserve = Math.max(mBufferSize, pos() + buf.readableBytes() - mReservedBytes);
        // Allocate enough space in the existing temporary block for the write.
        mBlockWorker.requestSpace(mSessionId, mBlockId, bytesToReserve);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    long append = mBlockWriter.append(buf);
    MetricsSystem.counter(MetricKey.WORKER_BYTES_WRITTEN_DIRECT.getName()).inc(append);
    MetricsSystem.meter(MetricKey.WORKER_BYTES_WRITTEN_DIRECT_THROUGHPUT.getName()).mark(append);
  }

  @Override
  public void cancel() throws IOException {
    mBlockWriter.close();
    try {
      mBlockWorker.abortBlock(mSessionId, mBlockId);
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      mBlockWorker.cleanupSession(mSessionId);
    }
  }

  @Override
  public void flush() {}

  @Override
  public void close() throws IOException {
    mBlockWriter.close();
    try {
      mBlockWorker.commitBlock(mSessionId, mBlockId,
          mOptions.getWriteType() == WriteType.ASYNC_THROUGH);
    } catch (Exception e) {
      mBlockWorker.cleanupSession(mSessionId);
      throw new IOException(e);
    }
  }

  /**
   * Creates an instance of {@link BlockWorkerDataWriter}.
   *
   * @param sessionId the session ID
   * @param blockId the block ID
   * @param options the outstream options
   * @param blockWriter the block writer
   * @param blockWorker the block worker
   * @param chunkSize the chunk size
   */
  private BlockWorkerDataWriter(long sessionId, long blockId, OutStreamOptions options,
      BlockWriter blockWriter, BlockWorker blockWorker, int chunkSize, long reservedBytes,
      AlluxioConfiguration conf) {
    mBlockWorker = blockWorker;
    mBlockWriter = blockWriter;
    mChunkSize = chunkSize;
    mBlockId = blockId;
    mOptions = options;
    mSessionId = sessionId;
    mReservedBytes = reservedBytes;
    mBufferSize = conf.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES);
  }
}
