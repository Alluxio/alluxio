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
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.OutStream;
import tachyon.conf.TachyonConf;
import tachyon.util.io.BufferUtils;

/**
 * Provides a stream API to write a block to Tachyon. An instance of this class can be obtained by
 * calling {@link TachyonBlockStore#getOutStream}. Only one BlockOutStream should be opened for a
 * block. This class is not thread safe and should only be used by one thread.
 *
 * The type of BlockOutStream returned will depend on the user configuration and cluster setup. A
 * {@link LocalBlockOutStream} is returned if the client is co-located with a Tachyon worker and
 * the user has enabled this optimization. Otherwise, a {@link RemoteBlockOutStream} will be
 * returned which will write the data through a Tachyon worker.
 */
public abstract class BufferedBlockOutStream extends OutStream {
  /** The block id of the block being written */
  protected final long mBlockId;
  /** Size of the block */
  protected final long mBlockSize;
  /** Block store context */
  protected final BSContext mContext;
  /** Java heap buffer to store writes before flushing them to the backing store. */
  protected final ByteBuffer mBuffer;

  /** If the stream is closed, this can only go from false to true */
  protected boolean mClosed;
  /** Number of bytes flushed to the under storage system. Implementing classes must update this. */
  protected long mFlushedBytes;
  /** Number of bytes written, including unflushed bytes */
  protected long mWrittenBytes;

  public BufferedBlockOutStream(long blockId, long blockSize) throws IOException {
    mBlockId = blockId;
    mBlockSize = blockSize;
    mBuffer = allocateBuffer();
    mClosed = false;
    mContext = BSContext.INSTANCE;
  }

  public ByteBuffer allocateBuffer() {
    TachyonConf conf = ClientContext.getConf();
    return ByteBuffer.allocate((int) conf.getBytes(Constants.USER_FILE_BUFFER_BYTES));
  }

  public long remaining() {
    return mBlockSize - mWrittenBytes;
  }

  @Override
  public void write(int b) throws IOException {
    checkIfClosed();
    Preconditions.checkState(mWrittenBytes + 1 <= mBlockSize, "Out of capacity.");
    if (mBuffer.position() >= mBuffer.limit()) {
      flush();
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
    checkIfClosed();
    Preconditions.checkArgument(b != null, "Buffer is null");
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length, String
        .format("Buffer length (%d), offset(%d), len(%d)", b.length, off, len));
    Preconditions.checkState(mWrittenBytes + len <= mBlockSize, "Out of capacity.");
    if (len == 0) {
      return;
    }

    if (mBuffer.position() > 0 && mBuffer.position() + len > mBuffer.limit()) {
      // Write the non-empty buffer if the new write will overflow it.
      flush();
    }

    if (len > mBuffer.limit() / 2) {
      // This write is "large", so do not write it to the buffer, but write it out directly to the
      // remote block.
      if (mBuffer.position() > 0) {
        // Make sure all bytes in the buffer are written out first, to prevent out-of-order writes.
        flush();
      }
      unBufferedWrite(b, off, len);
    } else {
      // Write the data to the buffer, and not directly to the remote block.
      mBuffer.put(b, off, len);
    }

    mWrittenBytes += len;
  }

  /**
   * Convenience method for checking the state of the stream.
   */
  protected void checkIfClosed() {
    Preconditions.checkState(!mClosed, "Cannot do operations on a closed BlockOutStream.");
  }

  /**
   * Writes the data in the byte array directly to the backing store. This should only be used
   * for writes which would not be able to be buffered.
   *
   * @param b the data that should be written
   * @param off the offset into the data to start writing from
   * @param len the length to write
   * @throws IOException if the write does not succeed
   */
  protected abstract void unBufferedWrite(byte[] b, int off, int len) throws IOException;
}
