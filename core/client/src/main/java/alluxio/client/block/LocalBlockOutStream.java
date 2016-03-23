/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block;

import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.util.io.FileUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.LocalFileBlockWriter;

import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a streaming API to write to an Alluxio block. This output stream will directly write the
 * input to a file in local Alluxio storage.
 */
@NotThreadSafe
public final class LocalBlockOutStream extends BufferedBlockOutStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final Closer mCloser;
  private final BlockWorkerClient mBlockWorkerClient;
  private final LocalFileBlockWriter mWriter;
  private long mReservedBytes;

  /**
   * Creates a new local block output stream.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param workerAddress the address of the local worker
   * @throws IOException if an I/O error occurs
   */
  public LocalBlockOutStream(long blockId, long blockSize, WorkerNetAddress workerAddress)
      throws IOException {
    super(blockId, blockSize);
    if (!NetworkAddressUtils.getLocalHostName(ClientContext.getConf())
        .equals(workerAddress.getHost())) {
      throw new IOException("The worker address " + workerAddress + " is not local");
    }

    mCloser = Closer.create();
    mBlockWorkerClient = mContext.acquireWorkerClient(workerAddress);

    try {
      long initialSize = ClientContext.getConf().getBytes(Constants.USER_FILE_BUFFER_BYTES);
      String blockPath = mBlockWorkerClient.requestBlockLocation(mBlockId, initialSize);
      mReservedBytes += initialSize;
      FileUtils.createBlockPath(blockPath);
      mWriter = new LocalFileBlockWriter(blockPath);
      mCloser.register(mWriter);
      // Change the permission of the temporary file in order that the worker can move it.
      FileUtils.changeLocalFileToFullPermission(blockPath);
      LOG.info("LocalBlockOutStream created new file block, block path: {}", blockPath);
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
    mCloser.close();
    try {
      mBlockWorkerClient.cancelBlock(mBlockId);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
    mContext.releaseWorkerClient(mBlockWorkerClient);
    mClosed = true;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    flush();
    mCloser.close();
    if (mWrittenBytes > 0) {
      try {
        mBlockWorkerClient.cacheBlock(mBlockId);
      } catch (AlluxioException e) {
        throw new IOException(e);
      }
      ClientContext.getClientMetrics().incBlocksWrittenLocal(1);
    }
    mContext.releaseWorkerClient(mBlockWorkerClient);
    mClosed = true;
  }

  @Override
  public void flush() throws IOException {
    int bytesToWrite = mBuffer.position();
    if (mReservedBytes < bytesToWrite) {
      mReservedBytes += requestSpace(bytesToWrite - mReservedBytes);
    }
    mBuffer.flip();
    mWriter.append(mBuffer);
    mBuffer.clear();
    mReservedBytes -= bytesToWrite;
    mFlushedBytes += bytesToWrite;
    ClientContext.getClientMetrics().incBytesWrittenLocal(bytesToWrite);
  }

  @Override
  protected void unBufferedWrite(byte[] b, int off, int len) throws IOException {
    if (mReservedBytes < len) {
      mReservedBytes += requestSpace(len - mReservedBytes);
    }
    mWriter.append(ByteBuffer.wrap(b, off, len));
    mReservedBytes -= len;
    mFlushedBytes += len;
    ClientContext.getClientMetrics().incBytesWrittenLocal(len);
  }

  private long requestSpace(long requestBytes) throws IOException {
    if (!mBlockWorkerClient.requestSpace(mBlockId, requestBytes)) {
      throw new IOException(ExceptionMessage.CANNOT_REQUEST_SPACE.getMessage());
    }
    return requestBytes;
  }
}
