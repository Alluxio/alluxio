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

import com.google.common.io.Closer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.UserConf;
import tachyon.util.CommonUtils;

/**
 * <code>BlockOutStream</code> implementation of TachyonFile. This class is not client facing.
 */
public class BlockOutStream extends OutStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final int mBlockIndex;
  private final long mBlockCapacityByte;
  private final long mBlockId;
  private final long mBlockOffset;
  private final boolean mPin;
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
   * @throws IOException
   */
  BlockOutStream(TachyonFile file, WriteType opType, int blockIndex) throws IOException {
    this(file, opType, blockIndex, UserConf.get().QUOTA_UNIT_BYTES);
  }

  /**
   * @param file the file the block belongs to
   * @param opType the OutStream's write type
   * @param blockIndex the index of the block in the file
   * @param initialBytes the initial size bytes that will be allocated to the block
   * @throws IOException
   */
  BlockOutStream(TachyonFile file, WriteType opType, int blockIndex, long initialBytes)
      throws IOException {
    super(file, opType);

    if (!opType.isCache()) {
      throw new IOException("BlockOutStream only support WriteType.CACHE");
    }

    mBlockIndex = blockIndex;
    mBlockCapacityByte = mFile.getBlockSizeByte();
    mBlockId = mFile.getBlockId(mBlockIndex);
    mBlockOffset = mBlockCapacityByte * blockIndex;
    mPin = mFile.needPin();

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

    mBuffer = ByteBuffer.allocate(mUserConf.FILE_BUFFER_BYTES + 4);
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

    if (mBuffer.position() + len >= mUserConf.FILE_BUFFER_BYTES && mBuffer.position() > 0) {
      appendCurrentBuffer(mBuffer.array(), 0, mBuffer.position());
      mBuffer.clear();
    }

    if (len >= mUserConf.FILE_BUFFER_BYTES) {
      appendCurrentBuffer(b, off, len);
    } else {
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

    if (mBuffer.position() >= mUserConf.FILE_BUFFER_BYTES) {
      appendCurrentBuffer(mBuffer.array(), 0, mBuffer.position());
      mBuffer.clear();
    }

    mBuffer.put((byte) (b & 0xFF));
    mWrittenBytes ++;
  }
}
