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

package tachyon.client.next.file;

import com.google.common.base.Preconditions;
import tachyon.client.next.ClientOptions;
import tachyon.client.next.block.BlockInStream;
import tachyon.thrift.FileInfo;

import java.io.IOException;
import java.util.List;

/**
 * Provides a streaming API to a file. This class wraps the BlockInStreams for each of the blocks
 * in the file and abstracts the switching between streams. The backing streams can read from
 * Tachyon space in the local machine, remote machines, or the under storage system.
 */
public class ClientFileInStream extends FileInStream {
  private final long mBlockSize;
  private final long mFileLength;
  private final ClientOptions mOptions;
  private final FSContext mContext;
  private final List<Long> mBlockIds;

  private boolean mClosed;
  private long mPos;
  private BlockInStream mCurrentBlockInStream;

  public ClientFileInStream(FileInfo info, ClientOptions options) {
    mBlockSize = info.getBlockSizeByte();
    mFileLength = info.getLength();
    mBlockIds = info.getBlockIds();
    mContext = FSContext.INSTANCE;
    mOptions = options;
    mClosed = false;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    if (mCurrentBlockInStream != null) {
      mCurrentBlockInStream.close();
    }
    mClosed = true;
  }

  @Override
  public int read() throws IOException {
    if (mPos >= mFileLength) {
      return -1;
    }

    checkAndAdvanceBlockInStream();
    // TODO: Determine if this is necessary
    int data = mCurrentBlockInStream.read();
    mPos ++;
    return data;
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
    } else if (mPos >= mFileLength) {
      return -1;
    }

    int tOff = off;
    int tLen = len;

    while (tLen > 0 && mPos < mFileLength) {
      checkAndAdvanceBlockInStream();

      int tRead = mCurrentBlockInStream.read(b, tOff, tLen);
      if (tRead == -1) {
        // mCurrentBlockInStream has reached its block boundary
        continue;
      }

      mPos += tRead;
      tLen -= tRead;
      tOff += tRead;
    }

    return len - tLen;
  }

  @Override
  public long remaining() {
    return mFileLength - mPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (mPos == pos) {
      return;
    }
    // TODO: Change these to use preconditions.
    if (pos < 0) {
      throw new IOException("Seek position is negative: " + pos);
    } else if (pos > mFileLength) {
      throw new IOException("Seek position is past EOF: " + pos + ", fileSize = " + mFileLength);
    }

    moveBlockInStream(pos);
    mCurrentBlockInStream.seek(mPos % mBlockSize);
  }

  @Override
  public long skip(long n) throws IOException {
    // TODO: Consider supporting backward skip
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(n, mFileLength - mPos);
    moveBlockInStream(mPos + toSkip);
    long shouldSkip = mPos % mBlockSize;
    if (shouldSkip != mCurrentBlockInStream.skip(shouldSkip)) {
      throw new IOException("The underlying BlockInStream could not skip " + shouldSkip);
    }
    return toSkip;
  }

  private void checkAndAdvanceBlockInStream() throws IOException {
    if (mCurrentBlockInStream == null) {
      mCurrentBlockInStream =
          mContext.getTachyonBS().getInStream(getBlockCurrentBlockId(), mOptions);
      return;
    }
    if (mCurrentBlockInStream.remaining() == 0) {
      mCurrentBlockInStream.close();
      mCurrentBlockInStream =
          mContext.getTachyonBS().getInStream(getBlockCurrentBlockId(), mOptions);
    }
  }

  private long getBlockCurrentBlockId() {
    int index = (int) (mPos / mBlockSize);
    Preconditions.checkState(index < mBlockIds.size(), "Current block index exceeds max index.");
    return mBlockIds.get(index);
  }

  private void moveBlockInStream(long newPos) throws IOException {
    long oldBlockId = getBlockCurrentBlockId();
    mPos = newPos;

    if (oldBlockId != getBlockCurrentBlockId()) {
      mCurrentBlockInStream.close();
      mCurrentBlockInStream =
          mContext.getTachyonBS().getInStream(getBlockCurrentBlockId(), mOptions);
    }
  }
}
