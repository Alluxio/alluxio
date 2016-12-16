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

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.AlluxioException;
import alluxio.metrics.MetricsSystem;
import alluxio.util.io.BufferUtils;
import alluxio.wire.LockBlockResult;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.LocalFileBlockReader;

import com.codahale.metrics.Counter;
import com.google.common.io.Closer;

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
  /** The block store context which provides block worker clients. */
  private final FileSystemContext mContext;
  /** The file reader to read a local block. */
  private final LocalFileBlockReader mReader;

  /**
   * Creates a new local block input stream.
   *
   * @param blockId the block id
   * @param blockSize the size of the block
   * @param workerNetAddress the address of the local worker
   * @param context the block store context to use for acquiring worker and master clients
   * @param options the instream options
   * @throws IOException if I/O error occurs
   */
  public LocalBlockInStream(long blockId, long blockSize, WorkerNetAddress workerNetAddress,
      FileSystemContext context, InStreamOptions options) throws IOException {
    super(blockId, blockSize);
    mContext = context;

    mCloser = Closer.create();
    try {
      mBlockWorkerClient = mCloser.register(mContext.createBlockWorkerClient(workerNetAddress));
      LockBlockResult result = mBlockWorkerClient.lockBlock(blockId);
      mReader = mCloser.register(new LocalFileBlockReader(result.getBlockPath()));
    } catch (AlluxioException e) {
      mCloser.close();
      throw new IOException(e);
    } catch (IOException e) {
      mCloser.close();
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
      mBlockWorkerClient.unlockBlock(mBlockId);
    } catch (Throwable e) { // must catch Throwable
      throw mCloser.rethrow(e); // IOException will be thrown as-is
    } finally {
      mClosed = true;
      mCloser.close();
      if (mBuffer != null && mBuffer.isDirect()) {
        BufferUtils.cleanDirectBuffer(mBuffer);
      }
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
