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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.metrics.MetricsSystem;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.LocalFileBlockWriter;

import com.codahale.metrics.Counter;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Provides a streaming API to write to an Alluxio block. This output stream will directly write the
 * input to a file in local Alluxio storage.
 */
@NotThreadSafe
public final class LocalBlockOutStream extends BufferedBlockOutStream {
  private final Closer mCloser;
  private final BlockWorkerClient mBlockWorkerClient;
  private final LocalFileBlockWriter mWriter;
  private long mReservedBytes;

  /**
   * Creates a new local block output stream.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param workerNetAddress the address of the local worker
   * @param context the file system context
   * @param options the options
   * @throws IOException if an I/O error occurs
   */
  public LocalBlockOutStream(long blockId,
      long blockSize,
      WorkerNetAddress workerNetAddress,
      FileSystemContext context,
      OutStreamOptions options) throws IOException {
    super(blockId, blockSize, context);
    if (!NetworkAddressUtils.getLocalHostName().equals(workerNetAddress.getHost())) {
      throw new IOException(ExceptionMessage.NO_LOCAL_WORKER.getMessage(workerNetAddress));
    }

    mCloser = Closer.create();
    try {
      mBlockWorkerClient = mCloser.register(context.createBlockWorkerClient(workerNetAddress));
      long initialSize = Configuration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES);
      String blockPath =
          mBlockWorkerClient.requestBlockLocation(mBlockId, initialSize, options.getWriteTier());
      mReservedBytes += initialSize;
      mWriter = new LocalFileBlockWriter(blockPath);
      mCloser.register(mWriter);
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
    } catch (Throwable e) { // must catch Throwable
      throw mCloser.rethrow(e); // IOException will be thrown as-is
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
      if (mWrittenBytes > 0) {
        mBlockWorkerClient.cacheBlock(mBlockId);
        Metrics.BLOCKS_WRITTEN_LOCAL.inc();
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
    int bytesToWrite = mBuffer.position();
    if (mReservedBytes < bytesToWrite) {
      long bytesToRequest = bytesToWrite - mReservedBytes;
      if (mBlockWorkerClient.requestSpace(mBlockId, bytesToRequest)) {
        mReservedBytes += bytesToRequest;
      }
    }
    mBuffer.flip();
    mWriter.append(mBuffer);
    mBuffer.clear();
    mReservedBytes -= bytesToWrite;
    mFlushedBytes += bytesToWrite;

    Metrics.BYTES_WRITTEN_LOCAL.inc(bytesToWrite);
  }

  @Override
  protected void unBufferedWrite(byte[] b, int off, int len) throws IOException {
    if (mReservedBytes < len) {
      long bytesToRequest = len - mReservedBytes;
      if (mBlockWorkerClient.requestSpace(mBlockId, bytesToRequest)) {
        mReservedBytes += bytesToRequest;
      }
    }
    mWriter.append(ByteBuffer.wrap(b, off, len));
    mReservedBytes -= len;
    mFlushedBytes += len;

    Metrics.BYTES_WRITTEN_LOCAL.inc(len);
  }

  /**
   * Class that contains metrics about LocalBlockOutStream.
   */
  @ThreadSafe
  private static final class Metrics {
    private static final Counter BLOCKS_WRITTEN_LOCAL =
        MetricsSystem.clientCounter("BlocksWrittenLocal");
    private static final Counter BYTES_WRITTEN_LOCAL =
        MetricsSystem.clientCounter("BytesWrittenLocal");

    private Metrics() {} // prevent instantiation.
  }
}
