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
   * @param options the output stream options
   * @return the {@link BlockWorkerDataWriter} created
   */
  public static BlockWorkerDataWriter create(final FileSystemContext context,
      long blockId, OutStreamOptions options) throws IOException {
    AlluxioConfiguration conf = context.getClusterConf();
    // TODO(lu) add new properties
    int chunkSize = (int) conf.getBytes(PropertyKey.USER_LOCAL_WRITER_CHUNK_SIZE_BYTES);
    long reservedBytes = conf.getBytes(PropertyKey.USER_FILE_RESERVED_BYTES);
    // TODO(lu) add metrics
    try {
      BlockWorker blockWorker = context.getInternalBlockWorker();
      WorkerBlockWriter blockWriter = WorkerBlockWriter.create(blockWorker, blockId,
          options.getWriteTier(), options.getMediumType(), reservedBytes,
          options.getWriteType() == WriteType.ASYNC_THROUGH);
      return new BlockWorkerDataWriter(blockWriter, chunkSize);
    } catch (Exception e) {
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
    mBlockWriter.append(buf);
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
