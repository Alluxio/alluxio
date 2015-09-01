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

package tachyon.client.next.block;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.client.RemoteBlockWriter;
import tachyon.client.next.ClientContext;
import tachyon.client.next.ClientOptions;
import tachyon.util.io.BufferUtils;
import tachyon.worker.next.WorkerClient;

/**
 * Provides a streaming API to write to a Tachyon block. This output stream will send the write
 * through a Tachyon worker which will then write the block to a file in Tachyon storage.
 */
public class RemoteBlockOutStream extends BlockOutStream {
  private final long mBlockId;
  private final long mBlockSize;
  private final BSContext mContext;
  private final ByteBuffer mBuffer;
  private final RemoteBlockWriter mRemoteWriter;
  private final WorkerClient mWorkerClient;

  private boolean mClosed;
  private long mFlushedBytes;
  private long mWrittenBytes;

  public RemoteBlockOutStream(long blockId, ClientOptions options) throws IOException {
    Preconditions.checkArgument(options.getCacheType().shouldCache(), "Remote Block OutStream "
        + "only supports CacheType CACHE.");
    mBlockId = blockId;
    mBlockSize = options.getBlockSize();
    mContext = BSContext.INSTANCE;
    // TODO: Get this value from the conf
    mBuffer = ByteBuffer.allocate(Constants.MB * 8);
    mRemoteWriter = RemoteBlockWriter.Factory.createRemoteBlockWriter(ClientContext.getConf());
    // TODO: This should be specified outside of options
    InetSocketAddress workerAddr =
        new InetSocketAddress(options.getLocation().getMHost(), options.getLocation()
            .getMSecondaryPort());
    mWorkerClient = mContext.acquireWorkerClient(workerAddr.getHostName());
    // TODO: Get the user ID
    mRemoteWriter.open(workerAddr, mBlockId, 1);
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    mRemoteWriter.close();
    mWorkerClient.cancelBlock(mBlockId);
    mContext.releaseWorkerClient(mWorkerClient);
    mClosed = true;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mRemoteWriter.close();
    if (mFlushedBytes == mBlockSize) {
      mWorkerClient.cacheBlock(mBlockId);
    } else {
      mWorkerClient.cancelBlock(mBlockId);
    }
    mContext.releaseWorkerClient(mWorkerClient);
    mClosed = true;
  }

  @Override
  public long remaining() {
    return mBlockSize - mWrittenBytes;
  }

  @Override
  public void write(int b) throws IOException {
    failIfClosed();
    Preconditions.checkState(mWrittenBytes + 1 <= mBlockSize, "Out of capacity.");
    if (mBuffer.position() >= mBuffer.limit()) {
      flushBufferToRemote();
    }
    BufferUtils.putIntByteBuffer(mBuffer, b);
    mWrittenBytes ++;
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
    Preconditions.checkState(mWrittenBytes + len <= mBlockSize, "Out of capacity.");
    if (len == 0) {
      return;
    }

    if (mBuffer.position() > 0 && mBuffer.position() + len > mBuffer.limit()) {
      // Write the non-empty buffer if the new write will overflow it.
      flushBufferToRemote();
    }

    if (len > mBuffer.limit() / 2) {
      // This write is "large", so do not write it to the buffer, but write it out directly to the
      // remote block.
      if (mBuffer.position() > 0) {
        // Make sure all bytes in the buffer are written out first, to prevent out-of-order writes.
        flushBufferToRemote();
      }
      writeToRemoteBlock(b, off, len);
    } else {
      // Write the data to the buffer, and not directly to the remote block.
      mBuffer.put(b, off, len);
    }

    mWrittenBytes += len;
  }

  private void failIfClosed() throws IOException {
    if (mClosed) {
      throw new IOException("Cannot do operations on a closed RemoteBlockOutStream");
    }
  }

  private void flushBufferToRemote() throws IOException {
    writeToRemoteBlock(mBuffer.array(), 0, mBuffer.position());
    mBuffer.clear();
  }

  private void writeToRemoteBlock(byte[] b, int off, int len) throws IOException {
    mRemoteWriter.write(b, off, len);
    mFlushedBytes += len;
  }
}
