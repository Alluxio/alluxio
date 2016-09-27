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

import alluxio.client.RemoteBlockWriter;
import alluxio.exception.AlluxioException;
import alluxio.metrics.MetricsSystem;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Counter;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Provides a streaming API to write to an Alluxio block. This output stream will send the write
 * through an Alluxio worker which will then write the block to a file in Alluxio storage.
 */
@NotThreadSafe
public final class RemoteBlockOutStream extends BufferedBlockOutStream {
  private final RemoteBlockWriter mRemoteWriter;
  private final BlockWorkerClient mBlockWorkerClient;

  /**
   * Creates a new block output stream on a specific address.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param address the address of the preferred worker
   * @param blockStoreContext the block store context
   * @throws IOException if I/O error occurs
   */
  public RemoteBlockOutStream(long blockId,
      long blockSize,
      WorkerNetAddress address,
      BlockStoreContext blockStoreContext) throws IOException {
    super(blockId, blockSize, blockStoreContext);
    mRemoteWriter = RemoteBlockWriter.Factory.create();
    mBlockWorkerClient = mContext.acquireWorkerClient(address);
    try {
      mRemoteWriter.open(mBlockWorkerClient.getDataServerAddress(), mBlockId,
          mBlockWorkerClient.getSessionId());
    } catch (IOException e) {
      mContext.releaseWorkerClient(mBlockWorkerClient);
      throw e;
    }
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    mRemoteWriter.close();
    try {
      mBlockWorkerClient.cancelBlock(mBlockId);
    } catch (AlluxioException e) {
      throw new IOException(e);
    } finally {
      releaseAndClose();
    }
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    flush();
    mRemoteWriter.close();
    if (mFlushedBytes > 0) {
      try {
        mBlockWorkerClient.cacheBlock(mBlockId);
      } catch (AlluxioException e) {
        throw new IOException(e);
      } finally {
        releaseAndClose();
      }
      Metrics.BLOCKS_WRITTEN_REMOTE.inc();
    } else {
      try {
        mBlockWorkerClient.cancelBlock(mBlockId);
      } catch (AlluxioException e) {
        throw new IOException(e);
      } finally {
        releaseAndClose();
      }
    }
  }

  @Override
  public void flush() throws IOException {
    writeToRemoteBlock(mBuffer.array(), 0, mBuffer.position());
    mBuffer.clear();
  }

  @Override
  protected void unBufferedWrite(byte[] b, int off, int len) throws IOException {
    writeToRemoteBlock(b, off, len);
  }

  private void writeToRemoteBlock(byte[] b, int off, int len) throws IOException {
    mRemoteWriter.write(b, off, len);
    mFlushedBytes += len;
    Metrics.BYTES_WRITTEN_REMOTE.inc(len);
  }

  /**
   * Releases {@link #mBlockWorkerClient} and sets {@link #mClosed} to true.
   */
  private void releaseAndClose() {
    mContext.releaseWorkerClient(mBlockWorkerClient);
    mClosed = true;
  }

  /**
   * Class that contains metrics about RemoteBlockOutStream.
   */
  @ThreadSafe
  private static final class Metrics {
    private static final Counter BLOCKS_WRITTEN_REMOTE =
        MetricsSystem.clientCounter("BlocksWrittenRemote");
    private static final Counter BYTES_WRITTEN_REMOTE =
        MetricsSystem.clientCounter("BytesWrittenRemote");

    private Metrics() {} // prevent instantiation
  }
}
