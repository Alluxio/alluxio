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
import alluxio.PropertyKey;
import alluxio.client.Cancelable;
import alluxio.exception.PreconditionMessage;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a stream API to write a block to Alluxio. An instance of this class can be obtained by
 * calling {@link AlluxioBlockStore#getOutStream}. Only one {@link BufferedBlockOutStream} should be
 * opened for a block.
 *
 * <p>
 * The type of {@link BufferedBlockOutStream} returned will depend on the user configuration and
 * cluster setup. A {@link LocalBlockOutStream} is returned if the client is co-located with an
 * Alluxio worker and the user has enabled this optimization. Otherwise,
 * {@link RemoteBlockOutStream} will be returned which will write the data through an Alluxio
 * worker.
 */
@NotThreadSafe
public abstract class BufferedBlockOutStream extends OutputStream implements Cancelable {
  /** The block id of the block being written. */
  protected final long mBlockId;
  /** Size of the block. */
  protected final long mBlockSize;
  /** Block store context. */
  protected final BlockStoreContext mContext;
  /** Java heap buffer to store writes before flushing them to the backing store. */
  protected final ByteBuffer mBuffer;

  /** If the stream is closed, this can only go from false to true. */
  protected boolean mClosed;
  /** Number of bytes flushed to the under storage system. Implementing classes must update this. */
  protected long mFlushedBytes;
  /** Number of bytes written, including unflushed bytes. */
  protected long mWrittenBytes;

  /**
   * Constructs a new {@link BufferedBlockOutStream}.
   *
   * @param blockId the id of the block
   * @param blockSize the size of the block
   * @param blockStoreContext the block store context
   */
  public BufferedBlockOutStream(long blockId, long blockSize, BlockStoreContext blockStoreContext) {
    mBlockId = blockId;
    mBlockSize = blockSize;
    mBuffer = allocateBuffer();
    mClosed = false;
    mContext = blockStoreContext;
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
    mWrittenBytes++;
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
    return ByteBuffer.allocate((int) Configuration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES));
  }
}
