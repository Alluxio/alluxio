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

package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;
import com.google.common.primitives.Ints;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;

/**
 * This implementation of {@link BlockOutStream} writes a single block to a remote worker.
 * This should not be created directly, but should be instantiated through one of the
 * BlockOutStream.get() methods.
 */
public class RemoteBlockOutStream extends BlockOutStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final int mBlockIndex;
  private final long mBlockId;
  private final long mBlockCapacityBytes;
  private final Closer mCloser;
  private final RemoteBlockWriter mRemoteWriter;

  // The local write buffer to accumulate client writes.
  private final ByteBuffer mBuffer;
  // The size of the write buffer in bytes.
  private final long mBufferBytes;
  // Total number of bytes written to block, but not necessarily flushed to the remote server.
  private long mWrittenBytes = 0;
  // True if stream is open.
  private boolean mOpen = false;

  /**
   * @param file the file the block belongs to
   * @param opType the OutStream's write type
   * @param blockIndex the index of the block in the file
   * @param tachyonConf the TachyonConf instance for this file output stream.
   * @throws IOException
   */
  RemoteBlockOutStream(TachyonFile file, WriteType opType, int blockIndex, TachyonConf tachyonConf)
      throws IOException {
    this(file, opType, blockIndex, tachyonConf.getBytes(Constants.USER_QUOTA_UNIT_BYTES,
        8 * Constants.MB), tachyonConf);
  }

  /**
   * @param file the file the block belongs to
   * @param opType the OutStream's write type
   * @param blockIndex the index of the block in the file
   * @param initialBytes the initial size bytes that will be allocated to the block
   * @param tachyonConf the TachyonConf instance for this file output stream.
   * @throws IOException
   */
  RemoteBlockOutStream(TachyonFile file, WriteType opType, int blockIndex, long initialBytes,
      TachyonConf tachyonConf) throws IOException {
    super(file, opType, tachyonConf);

    if (!opType.isCache()) {
      throw new IOException("RemoteBlockOutStream only support WriteType.CACHE");
    }

    mBlockIndex = blockIndex;
    mBlockCapacityBytes = mFile.getBlockSizeByte();
    mBlockId = mFile.getBlockId(mBlockIndex);
    mCloser = Closer.create();

    // Create a local buffer.
    mBufferBytes = mTachyonConf.getBytes(Constants.USER_FILE_BUFFER_BYTES, Constants.MB);
    long allocateBytes = mBufferBytes;
    mBuffer = ByteBuffer.allocate(Ints.checkedCast(allocateBytes));

    // Open the remote writer.
    mRemoteWriter =
        mCloser.register(RemoteBlockWriter.Factory.createRemoteBlockWriter(tachyonConf));
    mRemoteWriter.open(mTachyonFS.getWorkerDataServerAddress(), mBlockId, mTachyonFS.getUserId());
    mOpen = true;
  }

  /**
   * Write data to the remote block. This is synchronized to serialize writes to the block.
   *
   * @param bytes An array of bytes representing the source data.
   * @param offset The offset into the source array of bytes.
   * @param length The length of the data to write (in bytes).
   * @throws IOException
   */
  private synchronized void writeToRemoteBlock(byte[] bytes, int offset, int length)
      throws IOException {
    mRemoteWriter.write(bytes, offset, length);
  }

  // Flush the local buffer to the remote block.
  private void flushBuffer() throws IOException {
    writeToRemoteBlock(mBuffer.array(), 0, mBuffer.position());
    mBuffer.clear();
  }

  @Override
  public void cancel() throws IOException {
    if (mOpen) {
      mCloser.close();
      mOpen = false;
      if (mWrittenBytes > 0) {
        mTachyonFS.cancelBlock(mBlockId);
      }
      LOG.info(String.format("Canceled output of block. blockId(%d)", mBlockId));
    }
  }

  @Override
  public void close() throws IOException {
    if (mOpen) {
      if (mBuffer.position() > 0) {
        writeToRemoteBlock(mBuffer.array(), 0, mBuffer.position());
      }
      mCloser.close();
      if (mWrittenBytes > 0) {
        mTachyonFS.cacheBlock(mBlockId);
      }
      mOpen = false;
    }
  }

  @Override
  public void flush() throws IOException {
    if (mBuffer.position() > 0) {
      flushBuffer();
    }
  }

  /**
   * @return the block id of the block
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return the remaining space of the block, in bytes
   */
  @Override
  public long getRemainingSpaceBytes() {
    return mBlockCapacityBytes - mWrittenBytes;
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)
        || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException(String.format("Buffer length (%d), offset(%d), len(%d)",
          b.length, off, len));
    }
    if (!mOpen) {
      throw new IOException("Can not write cache.");
    }
    if (mWrittenBytes + len > mBlockCapacityBytes) {
      throw new IOException("Out of capacity.");
    }
    if (len == 0) {
      return;
    }

    if (mBuffer.position() > 0 && mBuffer.position() + len > mBufferBytes) {
      // Write the non-empty buffer if the new write will overflow it.
      flushBuffer();
    }

    if (len > mBufferBytes / 2) {
      // This write is "large", so do not write it to the buffer, but write it out directly to the
      // remote block.
      if (mBuffer.position() > 0) {
        // Make sure all bytes in the buffer are written out first, to prevent out-of-order writes.
        flushBuffer();
      }
      writeToRemoteBlock(b, off, len);
    } else {
      // Write the data to the buffer, and not directly to the remote block.
      mBuffer.put(b, off, len);
    }

    mWrittenBytes += len;
  }

  @Override
  public void write(int b) throws IOException {
    if (!mOpen) {
      throw new IOException("Can not write cache.");
    }
    if (mWrittenBytes + 1 > mBlockCapacityBytes) {
      throw new IOException("Out of capacity.");
    }
    if (mBuffer.position() >= mBufferBytes) {
      flushBuffer();
    }
    CommonUtils.putIntByteBuffer(mBuffer, b);
    mWrittenBytes ++;
  }
}
