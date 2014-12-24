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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.thrift.ClientLocationInfo;
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

  private long mInFileBytes = 0;
  private long mWrittenBytes = 0;
  private ClientLocationInfo mLocationInfo = null;

  private String mLocalFilePath = null;
  private RandomAccessFile mLocalFile = null;
  private FileChannel mLocalFileChannel = null;

  private ByteBuffer mBuffer = ByteBuffer.allocate(0);

  private boolean mCanWrite = false;
  private boolean mClosed = false;
  private boolean mCancel = false;

  /**
   * @param file the file the block belongs to
   * @param opType the OutStream's write type
   * @param blockIndex the index of the block in the file
   * @throws IOException
   */
  BlockOutStream(TachyonFile file, WriteType opType, int blockIndex) throws IOException {
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

    mBuffer = ByteBuffer.allocate(mUserConf.FILE_BUFFER_BYTES + 4);
  }

  private synchronized void appendCurrentBuffer(byte[] buf, int offset, int length)
      throws IOException {
    boolean reqResult = false;
    if (mLocationInfo == null) {
      mLocationInfo = mTachyonFS.requestSpace(length);
      reqResult = (mLocationInfo != null);
    } else {
      reqResult = mTachyonFS.requestSpace(mLocationInfo.getStorageDirId(), length);
    }
    if (!reqResult) {
      mCanWrite = false;

      String msg =
          "Local tachyon worker does not have enough " + "space (" + length + ") or no worker for "
              + mFile.mFileId + " " + mBlockId;

      throw new IOException(msg);
    }
    if (mLocalFilePath == null) {
      String localTempFolder = createUserLocalTempFolder();
      mLocalFilePath = CommonUtils.concat(localTempFolder, mBlockId);
      mLocalFile = new RandomAccessFile(mLocalFilePath, "rw");
      mLocalFileChannel = mLocalFile.getChannel();
      // change the permission of the temporary file in order that the worker can move it.
      CommonUtils.changeLocalFileToFullPermission(mLocalFilePath);
      // use the sticky bit, only the client and the worker can write to the block
      CommonUtils.setLocalFileStickyBit(mLocalFilePath);
      LOG.info(mLocalFilePath + " was created!");
    }

    MappedByteBuffer out = mLocalFileChannel.map(MapMode.READ_WRITE, mInFileBytes, length);
    out.put(buf, offset, length);
    mInFileBytes += length;
  }

  @Override
  public void cancel() throws IOException {
    mCancel = true;
    close();
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
      if (!mCancel && mBuffer.position() > 0) {
        appendCurrentBuffer(mBuffer.array(), 0, mBuffer.position());
      }

      if (mLocalFileChannel != null) {
        mLocalFileChannel.close();
        mLocalFile.close();
      }

      if (mCancel) {
        if (mLocationInfo != null) { // if file was written
          mTachyonFS.releaseSpace(mLocationInfo.getStorageDirId(),
              mWrittenBytes - mBuffer.position());
          new File(mLocalFilePath).delete();
          LOG.info("Canceled output of block " + mBlockId + ", deleted local file "
              + mLocalFilePath);
        }
      } else if (mLocationInfo != null) {
        mTachyonFS.cacheBlock(mLocationInfo.getStorageDirId(), mBlockId);
      }
    }
    mClosed = true;
  }

  /**
   * Create temporary folder for the user in some StorageDir
   * 
   * @return the path of the folder
   * @throws IOException
   */
  private String createUserLocalTempFolder() throws IOException {
    String localTempFolder = null;
    if (mLocationInfo != null) {
      localTempFolder = mTachyonFS.createAndGetUserLocalTempFolder(mLocationInfo.getPath());
    }

    if (localTempFolder == null) {
      mCanWrite = false;
      throw new IOException("Failed to create temp user folder for tachyon client.");
    }
    return localTempFolder;
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

    if (!mCanWrite) {
      throw new IOException("Can not write cache.");
    }
    if (mWrittenBytes + len > mBlockCapacityByte) {
      throw new IOException("Out of capacity.");
    }

    if (mBuffer.position() + len >= mUserConf.FILE_BUFFER_BYTES) {
      if (mBuffer.position() > 0) {
        appendCurrentBuffer(mBuffer.array(), 0, mBuffer.position());
        mBuffer.clear();
      }

      if (len > 0) {
        appendCurrentBuffer(b, off, len);
      }
    } else {
      mBuffer.put(b, off, len);
    }

    mWrittenBytes += len;
  }

  @Override
  public void write(int b) throws IOException {
    if (!mCanWrite) {
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
