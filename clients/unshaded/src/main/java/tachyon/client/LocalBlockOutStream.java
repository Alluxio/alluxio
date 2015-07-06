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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import com.google.common.primitives.Ints;
import com.google.common.io.Closer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;

/**
 * <code>LocalBlockOutStream</code> implementation of TachyonFile. This class is not client facing.
 */
public class LocalBlockOutStream extends BlockOutStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final int mBlockIndex;
  private final long mBlockCapacityByte;
  private final long mBlockId;
  private final long mBlockOffset;
  private final Closer mCloser = Closer.create();
  private final String mLocalFilePath;
  private final RandomAccessFile mLocalFile;
  private final FileChannel mLocalFileChannel;
  private final ByteBuffer mBuffer;

  private long mAvailableBytes = 0;
  private long mInFileBytes = 0;
  private long mWrittenBytes = 0;

  private boolean mCanWrite = false;
  private boolean mClosed = false;

  /**
   * @param file the file the block belongs to
   * @param opType the OutStream's write type
   * @param blockIndex the index of the block in the file
   * @param tachyonConf the TachyonConf instance for this file output stream.
   * @throws IOException
   */
  LocalBlockOutStream(TachyonFile file, WriteType opType, int blockIndex, TachyonConf tachyonConf)
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
  LocalBlockOutStream(TachyonFile file, WriteType opType, int blockIndex, long initialBytes,
                      TachyonConf tachyonConf) throws IOException {
    super(file, opType, tachyonConf);

    if (!opType.isCache()) {
      throw new IOException("LocalBlockOutStream only support WriteType.CACHE");
    }

    mBlockIndex = blockIndex;
    mBlockCapacityByte = mFile.getBlockSizeByte();
    mBlockId = mFile.getBlockId(mBlockIndex);
    mBlockOffset = mBlockCapacityByte * blockIndex;

    mCanWrite = true;

    if (!mTachyonFS.hasLocalWorker()) {
      mCanWrite = false;
      String msg = "The machine does not have any local worker.";
      throw new IOException(msg);
    }
    mLocalFilePath = mTachyonFS.getLocalBlockTemporaryPath(mBlockId, initialBytes);
    mLocalFile = mCloser.register(new RandomAccessFile(mLocalFilePath, "rw"));
    mLocalFileChannel = mCloser.register(mLocalFile.getChannel());
    // change the permission of the temporary file in order that the worker can move it.
    CommonUtils.changeLocalFileToFullPermission(mLocalFilePath);
    // use the sticky bit, only the client and the worker can write to the block
    CommonUtils.setLocalFileStickyBit(mLocalFilePath);
    LOG.info(mLocalFilePath + " was created!");
    mAvailableBytes += initialBytes;

    long allocateBytes = mTachyonConf.getBytes(Constants.USER_FILE_BUFFER_BYTES, Constants.MB) + 4L;
    mBuffer = ByteBuffer.allocate(Ints.checkedCast(allocateBytes));
  }

  private synchronized void appendCurrentBuffer(byte[] buf, int offset, int length)
      throws IOException {
    if (mAvailableBytes < length) {
      long bytesRequested = mTachyonFS.requestSpace(mBlockId, length - mAvailableBytes);
      if (bytesRequested + mAvailableBytes >= length) {
        mAvailableBytes += bytesRequested;
      } else {
        mCanWrite = false;
        throw new IOException(String.format("No enough space on local worker: fileId(%d)"
            + " blockId(%d) requestSize(%d)", mFile.mFileId, mBlockId, length - mAvailableBytes));
      }
    }

    MappedByteBuffer out = mLocalFileChannel.map(MapMode.READ_WRITE, mInFileBytes, length);
    out.put(buf, offset, length);
    mInFileBytes += length;
    mAvailableBytes -= length;
  }

  @Override
  public void cancel() throws IOException {
    if (!mClosed) {
      mCloser.close();
      mClosed = true;
      mTachyonFS.cancelBlock(mBlockId);
      LOG.info(String.format("Canceled output of block. blockId(%d) path(%s)", mBlockId,
          mLocalFilePath));
    }
  }

  /**
   * @return true if the stream can write and is not closed, otherwise false
   */
  public boolean canWrite() {
    return !mClosed && mCanWrite;
  }

  @Override
  public void close() throws IOException {
    if (!mClosed) {
      if (mBuffer.position() > 0) {
        appendCurrentBuffer(mBuffer.array(), 0, mBuffer.position());
      }
      mCloser.close();
      mTachyonFS.cacheBlock(mBlockId);
      mClosed = true;
    }
  }

  @Override
  public void flush() throws IOException {
    // Since this only writes to memory, this flush is not outside visible.
  }

  /**
   * @return the block id of the block
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return the block offset in the file.
   */
  public long getBlockOffset() {
    return mBlockOffset;
  }

  /**
   * @return the remaining space of the block, in bytes
   */
  @Override
  public long getRemainingSpaceByte() {
    return mBlockCapacityByte - mWrittenBytes;
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

    if (!canWrite()) {
      throw new IOException("Can not write cache.");
    }
    if (mWrittenBytes + len > mBlockCapacityByte) {
      throw new IOException("Out of capacity.");
    }

    long userFileBufferBytes =
        mTachyonConf.getBytes(Constants.USER_FILE_BUFFER_BYTES, Constants.MB);
    if (mBuffer.position() > 0 && mBuffer.position() + len > userFileBufferBytes) {
      // Write the non-empty buffer if the new write will overflow it.
      appendCurrentBuffer(mBuffer.array(), 0, mBuffer.position());
      mBuffer.clear();
    }

    if (len > userFileBufferBytes / 2) {
      // This write is "large", so do not write it to the buffer, but write it out directly to the
      // mapped file.
      if (mBuffer.position() > 0) {
        // Make sure all bytes in the buffer are written out first, to prevent out-of-order writes.
        appendCurrentBuffer(mBuffer.array(), 0, mBuffer.position());
        mBuffer.clear();
      }
      appendCurrentBuffer(b, off, len);
    } else if (len > 0) {
      // Write the data to the buffer, and not directly to the mapped file.
      mBuffer.put(b, off, len);
    }

    mWrittenBytes += len;
  }

  @Override
  public void write(int b) throws IOException {
    if (!canWrite()) {
      throw new IOException("Can not write cache.");
    }
    if (mWrittenBytes + 1 > mBlockCapacityByte) {
      throw new IOException("Out of capacity.");
    }

    if (mBuffer.position()
        >= mTachyonConf.getBytes(Constants.USER_FILE_BUFFER_BYTES, Constants.MB)) {
      appendCurrentBuffer(mBuffer.array(), 0, mBuffer.position());
      mBuffer.clear();
    }

    CommonUtils.putIntByteBuffer(mBuffer, b);
    mWrittenBytes ++;
  }
}
