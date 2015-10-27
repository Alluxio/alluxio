/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.block;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.exception.ExceptionMessage;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.FileUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.WorkerClient;

/**
 * Provides a streaming API to write to a Tachyon block. This output stream will directly write the
 * input to a file in local Tachyon storage. The instances of this class should only be used by one
 * thread and are not thread safe.
 */
public final class LocalBlockOutStream extends BufferedBlockOutStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final Closer mCloser;
  private final WorkerClient mWorkerClient;
  private final FileChannel mLocalFileChannel;

  private long mReservedBytes;

  /**
   * Creates a new local block output stream.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @throws IOException if an I/O error occurs
   */
  public LocalBlockOutStream(long blockId, long blockSize) throws IOException {
    super(blockId, blockSize);
    mCloser = Closer.create();
    mWorkerClient =
        mContext.acquireWorkerClient(NetworkAddressUtils.getLocalHostName(ClientContext.getConf()));

    try {
      long initialSize = ClientContext.getConf().getBytes(Constants.USER_FILE_BUFFER_BYTES);
      String blockPath = mWorkerClient.requestBlockLocation(mBlockId, initialSize);
      mReservedBytes += initialSize;
      FileUtils.createBlockPath(blockPath);
      RandomAccessFile localFile = mCloser.register(new RandomAccessFile(blockPath, "rw"));
      mLocalFileChannel = mCloser.register(localFile.getChannel());
      // Change the permission of the temporary file in order that the worker can move it.
      LOG.info("LocalBlockOutStream created new file block, block path: " + blockPath);
    } catch (IOException ioe) {
      mContext.releaseWorkerClient(mWorkerClient);
      throw ioe;
    }
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    mCloser.close();
    mWorkerClient.cancelBlock(mBlockId);
    mContext.releaseWorkerClient(mWorkerClient);
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
      mWorkerClient.cacheBlock(mBlockId);
      ClientContext.getClientMetrics().incBlocksWrittenLocal(1);
    }
    mContext.releaseWorkerClient(mWorkerClient);
    mClosed = true;
  }

  @Override
  public void flush() throws IOException {
    int bytesToWrite = mBuffer.position();
    if (mReservedBytes < bytesToWrite) {
      mReservedBytes += requestSpace(bytesToWrite - mReservedBytes);
    }
    MappedByteBuffer mappedBuffer =
        mLocalFileChannel.map(FileChannel.MapMode.READ_WRITE, mFlushedBytes, bytesToWrite);
    mappedBuffer.put(mBuffer.array(), 0, bytesToWrite);
    BufferUtils.cleanDirectBuffer(mappedBuffer);
    mReservedBytes -= bytesToWrite;
    mFlushedBytes += bytesToWrite;
    mBuffer.clear();
    ClientContext.getClientMetrics().incBytesWrittenLocal(bytesToWrite);
  }

  @Override
  protected void unBufferedWrite(byte[] b, int off, int len) throws IOException {
    if (mReservedBytes < len) {
      mReservedBytes += requestSpace(len - mReservedBytes);
    }
    MappedByteBuffer mappedBuffer = mLocalFileChannel.map(FileChannel.MapMode.READ_WRITE,
        mFlushedBytes, len);
    mappedBuffer.put(b, off, len);
    BufferUtils.cleanDirectBuffer(mappedBuffer);
    mReservedBytes -= len;
    mFlushedBytes += len;
    ClientContext.getClientMetrics().incBytesWrittenLocal(len);
  }

  private long requestSpace(long requestBytes) throws IOException {
    if (!mWorkerClient.requestSpace(mBlockId, requestBytes)) {
      throw new IOException(ExceptionMessage.CANNOT_REQUEST_SPACE.getMessage());
    }
    return requestBytes;
  }
}
