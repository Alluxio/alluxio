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
import java.io.OutputStream;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.client.Cancelable;
import tachyon.client.ClientContext;
import tachyon.conf.TachyonConf;
import tachyon.exception.PreconditionMessage;
import tachyon.util.io.BufferUtils;

/**
 * Provides a stream API to write a block to Tachyon. An instance of this class can be obtained by
 * calling {@link TachyonBlockStore#getOutStream}. Only one {@link BufferedBlockOutStream} should be
 * opened for a block.
 *
 * <p>
 * The type of {@link BufferedBlockOutStream} returned will depend on the user configuration and
 * cluster setup. A {@link LocalBlockOutStream} is returned if the client is co-located with a
 * Tachyon worker and the user has enabled this optimization. Otherwise, a
 * {@link RemoteBlockOutStream} will be returned which will write the data through a Tachyon worker.
 */
@NotThreadSafe
public abstract class BufferedBlockOutStream extends OutputStream implements Cancelable {
  /** The block id of the block being written */
  protected final long mBlockId;
  /** Size of the block */
  protected final long mBlockSize;
  /** Block store context */
  protected final BlockStoreContext mContext;
  /** Java heap buffer to store writes before flushing them to the backing store. */
  protected final ByteBuffer mBuffer;

  /** If the stream is closed, this can only go from false to true */
  protected boolean mClosed;
  /** Number of bytes flushed to the under storage system. Implementing classes must update this. */
  protected long mFlushedBytes;
  /** Number of bytes written, including unflushed bytes */
  protected long mWrittenBytes;

  /**
   * Constructs a new {@link BufferedBlockOutStream}.
   *
   * @param blockId the id of the block
   * @param blockSize the size of the block
   */
  public BufferedBlockOutStream(long blockId, long blockSize) {
    mBlockId = blockId;
    mBlockSize = blockSize;
    mBuffer = allocateBuffer();
    mClosed = false;
    mContext = BlockStoreContext.INSTANCE;
  }

  /**
   * @return the remaining size of the block
   */
  public long remaining() {
    return mBlockSize - mWrittenBytes;
  }

  @Override
  public void write(int b) throws IOException {
    checkIfClosed();
    Preconditions.checkState(mWrittenBytes + 1 <= mBlockSize, PreconditionMessage.ERR_END_OF_BLOCK);
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
    if (len == 0) {
      return;
    }

    // Write the non-empty buffer if the new write will overflow it.
    if (mBuffer.position() > 0 && mBuffer.position() + len > mBuffer.limit()) {
      flush();
    }

    // If this write is larger than half of buffer limit, then write it out directly
    // to the remote block. Before committing the new writes, need to make sure
    // all bytes in the buffer are written out first, to prevent out-of-order writes.
    // Otherwise, when the write is small, write the data to the buffer.
    if (len > mBuffer.limit() / 2) {
      if (mBuffer.position() > 0) {
        flush();
      }
      unBufferedWrite(b, off, len);
    } else {
      mBuffer.put(b, off, len);
    }

    mWrittenBytes += len;
  }

  /**
   * Convenience method for checking the state of the stream.
   */
  protected void checkIfClosed() {
    Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_OUT_STREAM);
  }

  /**
   * Writes the data in the byte array directly to the backing store. This should only be used for
   * writes which would not be able to be buffered.
   *
   * @param b the data that should be written
   * @param off the offset into the data to start writing from
   * @param len the length to write
   * @throws IOException if the write does not succeed
   */
  protected abstract void unBufferedWrite(byte[] b, int off, int len) throws IOException;

  /**
   * @return a newly allocated byte buffer of the user defined default size
   */
  private ByteBuffer allocateBuffer() {
    TachyonConf conf = ClientContext.getConf();
    return ByteBuffer.allocate((int) conf.getBytes(Constants.USER_FILE_BUFFER_BYTES));
  }
}
