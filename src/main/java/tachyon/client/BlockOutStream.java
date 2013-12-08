/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.client;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.util.CommonUtils;

/**
 * <code>BlockOutStream</code> implementation of TachyonFile. This class is not client facing.
 */
public class BlockOutStream extends OutStream {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final int BLOCK_INDEX;
  private final long BLOCK_CAPACITY_BYTE;
  private final long BLOCK_ID;
  private final long BLOCK_OFFSET;
  private final boolean PIN;

  private long mInFileBytes = 0;
  private long mWrittenBytes = 0;

  private String mLocalFilePath = null;
  private RandomAccessFile mLocalFile = null;
  private FileChannel mLocalFileChannel = null;

  private ByteBuffer mBuffer = ByteBuffer.allocate(0);

  private boolean mCanWrite = false;
  private boolean mClosed = false;
  private boolean mCancel = false;

  BlockOutStream(TachyonFile file, WriteType opType, int blockIndex) throws IOException {
    super(file, opType);

    if (!opType.isCache()) {
      throw new IOException("BlockOutStream only support WriteType.CACHE");
    }

    BLOCK_INDEX = blockIndex;
    BLOCK_CAPACITY_BYTE = FILE.getBlockSizeByte();
    BLOCK_ID = FILE.getBlockId(BLOCK_INDEX);
    BLOCK_OFFSET = BLOCK_CAPACITY_BYTE * blockIndex;
    PIN = FILE.needPin();

    mCanWrite = true;

    if (!TFS.hasLocalWorker()) {
      mCanWrite = false;
      String msg = "The machine does not have any local worker.";
      if (WRITE_TYPE.isMustCache()) {
        throw new IOException(msg);
      }
      LOG.warn("The machine does not have any local worker.");
      return;
    }

    File localFolder = TFS.createAndGetUserTempFolder();
    if (localFolder == null) {
      mCanWrite = false;
      String msg = "Failed to create temp user folder for tachyon client.";
      if (WRITE_TYPE.isMustCache()) {
        throw new IOException(msg);
      }
      LOG.warn(msg);
      return;
    }

    mLocalFilePath = localFolder.getPath() + "/" + BLOCK_ID;
    mLocalFile = new RandomAccessFile(mLocalFilePath, "rw");
    mLocalFileChannel = mLocalFile.getChannel();
    //change the permission of the temporary file in order that the worker can move it.
    CommonUtils.changeLocalFileToFullPermission(mLocalFilePath);
    //use the sticky bit, only the client and the worker can write to the block
    CommonUtils.setLocalFileStickyBit(mLocalFilePath);
    LOG.info(mLocalFilePath + " was created!");

    mBuffer = ByteBuffer.allocate(USER_CONF.FILE_BUFFER_BYTES + 4);
  }

  private synchronized void appendCurrentBuffer(byte[] buf, int offset,
      int length) throws IOException {
    if (!TFS.requestSpace(length)) {
      mCanWrite = false;

      String msg = "Local tachyon worker does not have enough " +
          "space (" + length + ") or no worker for " + FILE.FID + " " + BLOCK_ID;
      if (PIN) {
        TFS.outOfMemoryForPinFile(FILE.FID);
        throw new IOException(msg);
      }

      throw new IOException(msg);
    }

    MappedByteBuffer out = mLocalFileChannel.map(MapMode.READ_WRITE, mInFileBytes, length);
    out.put(buf, 0, length);
    mInFileBytes += length;
  }

  @Override
  public void write(int b) throws IOException {
    if (!mCanWrite) {
      throw new IOException("Can not write cache.");
    }
    if (mWrittenBytes + 1 > BLOCK_CAPACITY_BYTE) {
      throw new IOException("Out of capacity.");
    }

    if (mBuffer.position() >= USER_CONF.FILE_BUFFER_BYTES) {
      appendCurrentBuffer(mBuffer.array(), 0, mBuffer.position());
      mBuffer.clear();
    }

    mBuffer.put((byte) (b & 0xFF));
    mWrittenBytes ++;
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException(String.format(
          "Buffer length (%d), offset(%d), len(%d)", b.length, off, len));
    }

    if (!mCanWrite) {
      throw new IOException("Can not write cache.");
    }
    if (mWrittenBytes + len > BLOCK_CAPACITY_BYTE) {
      throw new IOException("Out of capacity.");
    }

    if (mBuffer.position() + len >= USER_CONF.FILE_BUFFER_BYTES) {
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
  public void flush() throws IOException {
    throw new IOException("Not supported yet.");
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
        TFS.releaseSpace(mWrittenBytes - mBuffer.position());
      } else {
        try {
          TFS.cacheBlock(BLOCK_ID);
        } catch (IOException e) {
          if (WRITE_TYPE == WriteType.MUST_CACHE) {
            throw e;
          }
        }
      }
    }
    mClosed = true;
  }

  @Override
  public void cancel() throws IOException {
    mCancel = true;
    close();
  }

  public boolean canWrite() {
    return !mClosed && mCanWrite;
  }

  public long getRemainingSpaceByte() {
    return BLOCK_CAPACITY_BYTE - mWrittenBytes;
  }

  public long getBlockId() {
    return BLOCK_ID;
  }

  public long getBlockOffset() {
    return BLOCK_OFFSET;
  }
}