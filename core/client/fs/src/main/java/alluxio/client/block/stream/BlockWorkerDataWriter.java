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
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.WorkerBlockWriter;

import io.netty.buffer.ByteBuf;

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
  private final WorkerBlockWriter mBlockWriter;
  private final int mChunkSize;

  /**
   * Creates an instance of {@link BlockWorkerDataWriter}.
   *
   * @param context the file system context
   * @param blockId the block ID
   * @param blockSize the block size in bytes
   * @param options the output stream options
   * @return the {@link BlockWorkerDataWriter} created
   */
  public static BlockWorkerDataWriter create(final FileSystemContext context,
      long blockId, long blockSize, OutStreamOptions options) throws IOException {
    AlluxioConfiguration conf = context.getClusterConf();
    int chunkSize = (int) conf.getBytes(PropertyKey.USER_LOCAL_WRITER_CHUNK_SIZE_BYTES);
    long reservedBytes = Math.min(blockSize, conf.getBytes(PropertyKey.USER_FILE_RESERVED_BYTES));
    BlockWorker blockWorker = context.getInternalBlockWorker();
    WorkerBlockWriter blockWriter = WorkerBlockWriter.create(blockWorker, blockId,
        options.getWriteTier(), options.getMediumType(), reservedBytes,
        options.getWriteType() == WriteType.ASYNC_THROUGH, true);
    return new BlockWorkerDataWriter(blockWriter, chunkSize);
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
    long append = mBlockWriter.append(buf);
    MetricsSystem.counter(MetricKey.WORKER_BYTES_WRITTEN_DIRECT.getName()).inc(append);
    MetricsSystem.meter(MetricKey.WORKER_BYTES_WRITTEN_DIRECT_THROUGHPUT.getName())
        .mark(append);
  }

  @Override
  public void cancel() throws IOException {
    mBlockWriter.cancel();
  }

  @Override
  public void flush() {}

  @Override
  public void close() throws IOException {
    mBlockWriter.close();
  }

  /**
   * Creates an instance of {@link BlockWorkerDataWriter}.
   *
   * @param blockWriter the block writer
   * @param chunkSize the chunk size
   */
  private BlockWorkerDataWriter(WorkerBlockWriter blockWriter, int chunkSize) {
    mBlockWriter = blockWriter;
    mChunkSize = chunkSize;
  }
}
