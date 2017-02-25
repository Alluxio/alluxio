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

package alluxio.client.block;

import alluxio.client.block.options.LockBlockOptions;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.AlluxioException;
import alluxio.metrics.MetricsSystem;
import alluxio.util.CommonUtils;
import alluxio.util.io.BufferUtils;
import alluxio.wire.LockBlockResult;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.LocalFileBlockReader;

import com.codahale.metrics.Counter;
import com.google.common.io.Closer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class provides a streaming API to read a block in Alluxio. The data will be directly read
 * from the local machine's storage.
 */
@NotThreadSafe
public final class LocalBlockInStream extends BufferedBlockInStream {
  /** Helper to manage closeables. */
  private final Closer mCloser;
  /** Client to communicate with the local worker. */
  private final BlockWorkerClient mBlockWorkerClient;
  /** The file reader to read a local block. */
  private final LocalFileBlockReader mReader;

  /**
   * Creates a new local block input stream.
   *
   * @param blockId the block id
   * @param blockSize the size of the block
   * @param workerNetAddress the address of the local worker
   * @param context the file system context
   * @param options the instream options
   * @return the {@link LocalBlockInStream} instance
   * @throws IOException if I/O error occurs
   */
  public static LocalBlockInStream create(long blockId, long blockSize,
      WorkerNetAddress workerNetAddress, FileSystemContext context, InStreamOptions options)
      throws IOException {
    Closer closer = Closer.create();
    BlockWorkerClient client;
    LockBlockResult result;
    try {
      client = closer.register(context.createBlockWorkerClient(workerNetAddress));
      result = client.lockBlock(blockId, LockBlockOptions.defaults());
    } catch (AlluxioException | IOException e) {
      CommonUtils.closeCloserIgnoreException(closer);
      throw CommonUtils.castToIOException(e);
    }
    return new LocalBlockInStream(client, blockId, blockSize, result.getBlockPath(),
        options);
  }

  /**
   * Creates a local block input stream. It requires the block to be locked before calling this.
   *
   * @param client the block worker client
   * @param blockId the block id
   * @param blockSize the size of the block
   * @param path the block file path
   * @param options the instream options
   * @return the {@link LocalBlockInStream} instance
   * @throws IOException if I/O error occurs
   */
  public static LocalBlockInStream createWithBlockLocked(BlockWorkerClient client, long blockId,
      long blockSize, String path, InStreamOptions options) throws IOException {
    return new LocalBlockInStream(client, blockId, blockSize, path, options);
  }

  /**
   * Creates a local block input stream. It requires the block to be locked before calling this.
   *
   * @param client the block worker client
   * @param blockId the block id
   * @param blockSize the size of the block
   * @param path the block file path
   * @param options the instream options
   * @throws IOException if I/O error occurs
   */
  private LocalBlockInStream(BlockWorkerClient client, long blockId,
      long blockSize, String path, InStreamOptions options) throws IOException {
    super(blockId, blockSize);
    mBlockWorkerClient = client;

    mCloser = Closer.create();
    mCloser.register(client);
    mCloser.register(new Closeable() {
      @Override
      public void close() throws IOException {
        mBlockWorkerClient.unlockBlock(mBlockId);
      }
    });
    try {
      mReader = mCloser.register(new LocalFileBlockReader(path));
    } catch (IOException e) {
      CommonUtils.closeCloserIgnoreException(mCloser);
      throw e;
    }
  }

  @Override
  public void seek(long pos) throws IOException {
    super.seek(pos);
    Metrics.SEEKS_LOCAL.inc();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    try {
      if (mBlockIsRead) {
        mBlockWorkerClient.accessBlock(mBlockId);
        Metrics.BLOCKS_READ_LOCAL.inc();
      }
      // Note that the block is unlocked by closing mCloser.
    } catch (Throwable e) { // must catch Throwable
      throw mCloser.rethrow(e); // IOException will be thrown as-is
    } finally {
      mClosed = true;
      if (mBuffer != null && mBuffer.isDirect()) {
        BufferUtils.cleanDirectBuffer(mBuffer);
      }
      mCloser.close();
    }
  }

  @Override
  protected void bufferedRead(int len) throws IOException {
    if (mBuffer.isDirect()) { // Buffer may not be direct on initialization
      BufferUtils.cleanDirectBuffer(mBuffer);
    }
    mBuffer = mReader.read(getPosition(), len);
  }

  @Override
  public int directRead(byte[] b, int off, int len) throws IOException {
    ByteBuffer buf = mReader.read(getPosition(), len);
    buf.get(b, off, len);
    BufferUtils.cleanDirectBuffer(buf);
    return len;
  }

  @Override
  protected void incrementBytesReadMetric(int bytes) {
    Metrics.BYTES_READ_LOCAL.inc(bytes);
  }

  /**
   * Class that contains metrics about LocalBlockInStream.
   */
  @ThreadSafe
  private static final class Metrics {
    private static final Counter BLOCKS_READ_LOCAL = MetricsSystem.clientCounter("BlocksReadLocal");
    private static final Counter BYTES_READ_LOCAL = MetricsSystem.clientCounter("BytesReadLocal");
    private static final Counter SEEKS_LOCAL = MetricsSystem.clientCounter("SeeksLocal");

    private Metrics() {}  // prevent instantiation.
  }
}
