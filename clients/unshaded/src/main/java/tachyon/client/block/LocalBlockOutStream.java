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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.FileUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.WorkerClient;

/**
 * Provides a streaming API to write to a Tachyon block. This output stream will directly write
 * the input to a file in local Tachyon storage.
 */
public class LocalBlockOutStream extends BlockOutStream {
  private final Closer mCloser;
  private final long mBlockSize;
  private final long mBlockId;
  private final BSContext mContext;
  private final WorkerClient mWorkerClient;
  private final String mBlockPath;
  private final RandomAccessFile mLocalFile;
  private final FileChannel mLocalFileChannel;

  private boolean mClosed;
  private long mAvailableBytes;
  private long mWrittenBytes;

  /**
   * Creates a new local block output stream.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @throws IOException if an I/O error occurs
   */
  public LocalBlockOutStream(long blockId, long blockSize) throws IOException {
    mCloser = Closer.create();
    mBlockSize = blockSize;
    mBlockId = blockId;
    mContext = BSContext.INSTANCE;

    mWorkerClient =
        mContext.acquireWorkerClient(NetworkAddressUtils.getLocalHostName(ClientContext.getConf()));

    try {
      // TODO: Get the initial size from the configuration
      long initialSize = ClientContext.getConf().getBytes(Constants.USER_FILE_BUFFER_BYTES);
      mBlockPath = mWorkerClient.requestBlockLocation(blockId, initialSize);
      // TODO: Handle this in the worker?
      FileUtils.createBlockPath(mBlockPath);
      mAvailableBytes += initialSize;

      mLocalFile = mCloser.register(new RandomAccessFile(mBlockPath, "rw"));
      mLocalFileChannel = mCloser.register(mLocalFile.getChannel());
      // change the permission of the temporary file in order that the worker can move it.
      FileUtils.changeLocalFileToFullPermission(mBlockPath);
      // TODO: Add a log message to indicate the file creation
    } catch (IOException ioe) {
      mContext.releaseWorkerClient(mWorkerClient);
      throw ioe;
    }
  }

  private void failIfClosed() throws IOException {
    if (mClosed) {
      throw new IOException("Cannot write to a closed OutputStream");
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
    }
    mContext.releaseWorkerClient(mWorkerClient);
    mClosed = true;
  }

  @Override
  public long remaining() {
    return mBlockSize - mWrittenBytes;
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    failIfClosed();
    Preconditions.checkArgument(b != null, "Buffer is null");
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length, String
        .format("Buffer length (%d), offset(%d), len(%d)", b.length, off, len));

    if (mAvailableBytes < len) {
      mAvailableBytes += requestSpace(len - mAvailableBytes);
    }

    MappedByteBuffer out =
        mLocalFileChannel.map(FileChannel.MapMode.READ_WRITE, mWrittenBytes, len);
    out.put(b, off, len);
    BufferUtils.cleanDirectBuffer(out);
    mAvailableBytes -= len;
    mWrittenBytes += len;
  }

  @Override
  public void write(int b) throws IOException {
    failIfClosed();
    if (mWrittenBytes + 1 > mBlockSize) {
      // TODO: Handle this error better
      throw new IOException("Block capacity exceeded.");
    }
    if (mAvailableBytes < 1) {
      mAvailableBytes += requestSpace(1);
    }
    ByteBuffer buf = ByteBuffer.allocate(1);
    BufferUtils.putIntByteBuffer(buf, b);
    buf.flip();
    mLocalFileChannel.write(buf);
    mAvailableBytes --;
    mWrittenBytes ++;
  }

  private long requestSpace(long requestBytes) throws IOException {
    if (!mWorkerClient.requestSpace(mBlockId, requestBytes)) {
      throw new IOException("Unable to request space from worker.");
    }
    return requestBytes;
  }
}
