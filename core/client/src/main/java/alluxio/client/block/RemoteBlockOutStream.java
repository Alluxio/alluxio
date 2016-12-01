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

import alluxio.Constants;
import alluxio.client.RemoteBlockWriter;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.AlluxioException;
import alluxio.metrics.MetricsSystem;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Counter;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Provides a streaming API to write to an Alluxio block. This output stream will send the write
 * through an Alluxio worker which will then write the block to a file in Alluxio storage.
 */
@NotThreadSafe
public final class RemoteBlockOutStream extends BufferedBlockOutStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final RemoteBlockWriter mRemoteWriter;
  private final BlockWorkerClient mBlockWorkerClient;
  private final Closer mCloser;

  /**
   * Creates a new block output stream on a specific address.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param address the address of the preferred worker
   * @param blockStoreContext the block store context
   * @param options the options
   * @throws IOException if I/O error occurs
   */
  public RemoteBlockOutStream(long blockId,
      long blockSize,
      WorkerNetAddress address,
      BlockStoreContext blockStoreContext,
      OutStreamOptions options) throws IOException {
    super(blockId, blockSize, blockStoreContext);
    mCloser = Closer.create();
    try {
      mBlockWorkerClient = mCloser.register(mContext.createWorkerClient(address));
      mRemoteWriter = mCloser.register(RemoteBlockWriter.Factory.create());

      mRemoteWriter.open(mBlockWorkerClient.getDataServerAddress(), mBlockId,
          mBlockWorkerClient.getSessionId());
    } catch (IOException e) {
      mCloser.close();
      throw e;
    }
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    try {
      mBlockWorkerClient.cancelBlock(mBlockId);
    } catch (AlluxioException e) {
      throw mCloser.rethrow(new IOException(e));
    } finally {
      mClosed = true;
      mCloser.close();
    }
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }

    try {
      flush();
      if (mFlushedBytes > 0) {
        mBlockWorkerClient.cacheBlock(mBlockId);
        Metrics.BLOCKS_WRITTEN_REMOTE.inc();
      } else {
        mBlockWorkerClient.cancelBlock(mBlockId);
      }
    } catch (AlluxioException e) {
      throw mCloser.rethrow(new IOException(e));
    } catch (Throwable e) { // must catch Throwable
      throw mCloser.rethrow(e); // IOException will be thrown as-is
    } finally {
      mClosed = true;
      mCloser.close();
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
