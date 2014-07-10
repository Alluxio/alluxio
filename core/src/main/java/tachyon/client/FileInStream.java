/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.client;

import java.io.IOException;

/**
 * FileInStream implementation of TachyonFile.
 */
public class FileInStream extends InStream {
  private final long FILE_LENGTH;
  private final long BLOCK_CAPACITY;

  private long mCurrentPosition;
  private int mCurrentBlockIndex;
  private BlockInStream mCurrentBlockInStream;
  private long mCurrentBlockLeft;

  private boolean mClosed = false;

  private Object mUFSConf = null;

  /**
   * @param file
   *          the file to be read
   * @param opType
   *          the InStream's read type
   * @throws IOException
   */
  public FileInStream(TachyonFile file, ReadType opType) throws IOException {
    this(file, opType, null);
  }

  /**
   * @param file
   *          the file to be read
   * @param opType
   *          the InStream's read type
   * @param ufsConf
   *          the under file system configuration
   * @throws IOException
   */
  public FileInStream(TachyonFile file, ReadType opType, Object ufsConf) throws IOException {
    super(file, opType);

    FILE_LENGTH = file.length();
    BLOCK_CAPACITY = file.getBlockSizeByte();

    mCurrentPosition = 0;
    mCurrentBlockIndex = -1;
    mCurrentBlockInStream = null;
    mCurrentBlockLeft = 0;

    mUFSConf = ufsConf;
  }

  private void checkAndAdvanceBlockInStream() throws IOException {
    if (mCurrentBlockLeft == 0) {
      if (mCurrentBlockInStream != null) {
        mCurrentBlockInStream.close();
      }

      mCurrentBlockIndex = getCurrentBlockIndex();
      mCurrentBlockInStream = BlockInStream.get(FILE, READ_TYPE, mCurrentBlockIndex, mUFSConf);
      mCurrentBlockLeft = BLOCK_CAPACITY;
    }
  }

  @Override
  public void close() throws IOException {
    if (!mClosed && mCurrentBlockInStream != null) {
      mCurrentBlockInStream.close();
    }

    mClosed = true;
  }

  private int getCurrentBlockIndex() {
    return (int) (mCurrentPosition / BLOCK_CAPACITY);
  }

  @Override
  public int read() throws IOException {
    if (mCurrentPosition >= FILE_LENGTH) {
      return -1;
    }

    checkAndAdvanceBlockInStream();

    mCurrentPosition ++;
    mCurrentBlockLeft --;
    return mCurrentBlockInStream.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    int tOff = off;
    int tLen = len;

    while (tLen > 0 && mCurrentPosition < FILE_LENGTH) {
      checkAndAdvanceBlockInStream();

      int tRead = mCurrentBlockInStream.read(b, tOff, tLen);
      if (tRead == -1) {
        // mCurrentBlockInStream has reached its block boundary
        continue;
      }

      mCurrentPosition += tRead;
      mCurrentBlockLeft -= tRead;
      tLen -= tRead;
      tOff += tRead;
    }

    return len - tLen;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (mCurrentPosition == pos) {
      return;
    }
    if (pos < 0) {
      throw new IOException("pos is negative: " + pos);
    }

    if ((int) (pos / BLOCK_CAPACITY) != mCurrentBlockIndex) {
      mCurrentBlockIndex = (int) (pos / BLOCK_CAPACITY);
      if (mCurrentBlockInStream != null) {
        mCurrentBlockInStream.close();
      }
      mCurrentBlockInStream = BlockInStream.get(FILE, READ_TYPE, mCurrentBlockIndex, mUFSConf);
    }
    mCurrentBlockInStream.seek(pos % BLOCK_CAPACITY);
    mCurrentPosition = pos;
    mCurrentBlockLeft = BLOCK_CAPACITY - (pos % BLOCK_CAPACITY);
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    long ret = n;
    if (mCurrentPosition + n >= FILE.length()) {
      ret = FILE.length() - mCurrentPosition;
      mCurrentPosition += ret;
    } else {
      mCurrentPosition += n;
    }

    int tBlockIndex = (int) (mCurrentPosition / BLOCK_CAPACITY);
    if (tBlockIndex != mCurrentBlockIndex) {
      if (mCurrentBlockInStream != null) {
        mCurrentBlockInStream.close();
      }

      mCurrentBlockIndex = tBlockIndex;
      mCurrentBlockInStream = BlockInStream.get(FILE, READ_TYPE, mCurrentBlockIndex, mUFSConf);
      long shouldSkip = mCurrentPosition % BLOCK_CAPACITY;
      long skip = mCurrentBlockInStream.skip(shouldSkip);
      mCurrentBlockLeft = BLOCK_CAPACITY - skip;
      if (skip != shouldSkip) {
        throw new IOException("The underlayer BlockInStream only skip " + skip + " instead of "
            + shouldSkip);
      }
    } else {
      long skip = mCurrentBlockInStream.skip(ret);
      if (skip != ret) {
        throw new IOException("The underlayer BlockInStream only skip " + skip + " instead of "
            + ret);
      }
    }

    return ret;
  }
}