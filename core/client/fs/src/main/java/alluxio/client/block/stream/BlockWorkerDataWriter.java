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

import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.SessionIdUtils;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockWriter;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A data writer that issues from a client embedded in a worker
 * to write data to the worker directly via internal communication channel.
 *
 * This data writer is similar to write to local worker via {@link GrpcDataWriter}
 * except that all communication with the local worker is via internal method call
 * instead of external RPC frameworks.
 */
@NotThreadSafe
public final class BlockWorkerDataWriter implements DataWriter {
  private static final Logger LOG = LoggerFactory.getLogger(BlockWorkerDataWriter.class);

  private final long mBlockId;
  private final BlockWorker mBlockWorker;
  private final long mChunkSize;
  private final long mFileBufferBytes;
  private final long mSessionId;

  private BlockWriter mBlockWriter;

  /** The position to write the next byte at. */
  private long mPos;
  /** The number of bytes reserved on the block worker to hold the block. */
  private long mPosReserved;

  /**
   * Creates an instance of {@link BlockWorkerDataWriter}.
   *
   * @param context the file system context
   * @param blockId the block ID
   * @param options the output stream options
   * @return the {@link BlockWorkerDataWriter} created
   */
  public static BlockWorkerDataWriter create(final FileSystemContext context,
      long blockId, OutStreamOptions options) throws IOException {
    AlluxioConfiguration conf = context.getClusterConf();
    long chunkSize = conf.getBytes(PropertyKey.USER_LOCAL_WRITER_CHUNK_SIZE_BYTES);
    long fileBufferBytes = conf.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES);
    try {
      BlockWorker blockWorker = context.getInternalBlockWorker();
      long sessionId = SessionIdUtils.createSessionId();
      blockWorker.createBlockRemote(sessionId, blockId,
          options.getWriteTier(), options.getMediumType(), chunkSize);
      return new BlockWorkerDataWriter(blockWorker, sessionId, blockId, chunkSize, fileBufferBytes);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public long pos() {
    return mPos;
  }

  @Override
  public int chunkSize() {
    return (int) mChunkSize;
  }

  @Override
  public void writeChunk(final ByteBuf buf) throws IOException {
    try {
      int sz = buf.readableBytes();
      if (mPosReserved < mPos + sz) {
        long bytesToReserve = Math.max(mFileBufferBytes, mPos + sz - mPosReserved);
        // Allocate enough space in the existing temporary block for the write.
        mBlockWorker.requestSpace(mSessionId, mBlockId, bytesToReserve);
        mPosReserved += bytesToReserve;
      }
      if (mBlockWriter == null) {
        mBlockWriter = mBlockWorker.getTempBlockWriterRemote(mSessionId, mBlockId);
      }
      Preconditions.checkState(mBlockWriter != null);
      mPos += sz;
      Preconditions.checkState(mBlockWriter.append(buf) == sz);
      MetricsSystem.counter(MetricKey.CLIENT_BYTES_WRITTEN_LOCAL.getName()).inc(sz);
      MetricsSystem.meter(MetricKey.CLIENT_BYTES_WRITTEN_LOCAL_THROUGHPUT.getName()).mark(sz);
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      buf.release();
    }
  }

  @Override
  public void cancel() throws IOException {
    if (mBlockWriter != null) {
      mBlockWriter.close();
    }
    try {
      mBlockWorker.abortBlock(mSessionId, mBlockId);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void flush() {}

  @Override
  public void close() throws IOException {
    if (mBlockWriter != null) {
      mBlockWriter.close();
    }
    try {
      mBlockWorker.commitBlock(mSessionId, mBlockId, false);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Creates an instance of {@link BlockWorkerDataWriter}.
   *
   * @param blockWorker the block worker
   * @param sessionId the session id
   * @param blockId id the block id
   * @param chunkSize the chuck size
   * @param fileBufferBytes the file buffer size in bytes
   */
  private BlockWorkerDataWriter(BlockWorker blockWorker,
      long sessionId, long blockId, long chunkSize, long fileBufferBytes) {
    mBlockWorker = blockWorker;
    mSessionId = sessionId;
    mBlockId = blockId;
    mChunkSize = chunkSize;
    mFileBufferBytes = fileBufferBytes;
  }
}
