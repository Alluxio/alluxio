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

package tachyon.client.file;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;

import tachyon.client.ClientOptions;
import tachyon.client.InStream;
import tachyon.client.block.BlockInStream;
import tachyon.client.block.BlockOutStream;
import tachyon.client.block.LocalBlockInStream;
import tachyon.master.block.BlockId;
import tachyon.thrift.FileInfo;

/**
 * Provides a streaming API to read a file. This class wraps the BlockInStreams for each of the
 * blocks in the file and abstracts the switching between streams. The backing streams can read from
 * Tachyon space in the local machine, remote machines, or the under storage system.
 */
public final class FileInStream extends InStream {
  private final boolean mShouldCache;
  private final long mBlockSize;
  private final long mFileLength;
  private final FSContext mContext;
  private final List<Long> mBlockIds;
  private final String mUfsPath;
  private final ClientOptions mOptions;

  private boolean mClosed;
  private boolean mShouldCacheCurrentBlock;
  private long mPos;
  private BlockInStream mCurrentBlockInStream;
  private BlockOutStream mCurrentCacheStream;

  public FileInStream(FileInfo info, ClientOptions options) {
    mBlockSize = info.getBlockSizeByte();
    mFileLength = info.getLength();
    mBlockIds = info.getBlockIds();
    mUfsPath = info.getUfsPath();
    mOptions = options;
    mContext = FSContext.INSTANCE;
    mShouldCache = options.getCacheType().shouldCache();
    mShouldCacheCurrentBlock = mShouldCache;
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
    closeCacheStream();
    mClosed = true;
  }

  @Override
  public int read() throws IOException {
    if (mPos >= mFileLength) {
      return -1;
    }

    checkAndAdvanceBlockInStream();
    int data = mCurrentBlockInStream.read();
    mPos ++;
    if (mShouldCacheCurrentBlock) {
      try {
        mCurrentCacheStream.write(data);
      } catch (IOException ioe) {
        // TODO: Log debug maybe?
        mShouldCacheCurrentBlock = false;
      }
    }
    return data;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, "Buffer is null");
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length, String
        .format("Buffer length (%d), offset(%d), len(%d)", b.length, off, len));
    if (len == 0) {
      return 0;
    } else if (mPos >= mFileLength) {
      return -1;
    }

    int tOff = off;
    int tLen = len;

    while (tLen > 0 && mPos < mFileLength) {
      checkAndAdvanceBlockInStream();

      int tRead = mCurrentBlockInStream.read(b, tOff, tLen);
      if (tRead > 0 && mShouldCacheCurrentBlock) {
        try {
          mCurrentCacheStream.write(b, tOff, tRead);
        } catch (IOException ioe) {
          // TODO: Log debug maybe?
          mShouldCacheCurrentBlock = false;
        }
      }
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
    Preconditions.checkArgument(pos >= 0, "Seek position is negative: " + pos);
    Preconditions.checkArgument(pos <= mFileLength, "Seek position is past EOF: " + pos
        + ", fileSize = " + mFileLength);

    moveBlockInStream(pos);
    checkAndAdvanceBlockInStream();
    mCurrentBlockInStream.seek(mPos % mBlockSize);
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(n, mFileLength - mPos);
    long newPos = mPos + toSkip;
    long toSkipInBlock = ((newPos / mBlockSize) > mPos / mBlockSize) ? newPos % mBlockSize :
        toSkip;
    moveBlockInStream(newPos);
    checkAndAdvanceBlockInStream();
    if (toSkipInBlock != mCurrentBlockInStream.skip(toSkipInBlock)) {
      throw new IOException("The underlying BlockInStream could not skip " + toSkip);
    }
    return toSkip;
  }

  private void checkAndAdvanceBlockInStream() throws IOException {
    long currentBlockId = getBlockCurrentBlockId();
    if (mCurrentBlockInStream == null || mCurrentBlockInStream.remaining() == 0) {
      if (mCurrentBlockInStream != null) {
        mCurrentBlockInStream.close();
      }
      try {
        mCurrentBlockInStream = mContext.getTachyonBS().getInStream(currentBlockId);
        mShouldCacheCurrentBlock =
            !(mCurrentBlockInStream instanceof LocalBlockInStream) && mShouldCache;
      } catch (IOException ioe) {
        if (null == mUfsPath || mUfsPath.isEmpty()) {
          throw ioe;
        }
        // TODO: Maybe debug log here
        long blockStart = BlockId.getSequenceNumber(currentBlockId) * mBlockSize;
        mCurrentBlockInStream = new UnderStoreFileInStream(blockStart, mBlockSize, mUfsPath);
        mShouldCacheCurrentBlock = mShouldCache;
      }
      closeCacheStream();
      if (mShouldCacheCurrentBlock) {
        try {
          mCurrentCacheStream =
              mContext.getTachyonBS().getOutStream(currentBlockId, -1, null);
        } catch (IOException ioe) {
          // TODO: Maybe debug log here
          mShouldCacheCurrentBlock = false;
        }
      }
    }
  }

  private void closeCacheStream() throws IOException {
    if (mCurrentCacheStream == null) {
      return;
    }
    if (mCurrentCacheStream.remaining() == 0) {
      mCurrentCacheStream.close();
    } else {
      mCurrentCacheStream.cancel();
    }
  }

  private long getBlockCurrentBlockId() {
    int index = (int) (mPos / mBlockSize);
    Preconditions.checkState(index < mBlockIds.size(), "Current block index exceeds max index.");
    return mBlockIds.get(index);
  }

  // TODO: This can be combined with check and advance
  private void moveBlockInStream(long newPos) throws IOException {
    long oldBlockId = getBlockCurrentBlockId();
    mPos = newPos;
    closeCacheStream();
    long currentBlockId = getBlockCurrentBlockId();

    if (oldBlockId != currentBlockId) {
      if (mCurrentBlockInStream != null) {
        mCurrentBlockInStream.close();
      }
      try {
        mCurrentBlockInStream = mContext.getTachyonBS().getInStream(currentBlockId);
        mShouldCacheCurrentBlock =
            !(mCurrentBlockInStream instanceof LocalBlockInStream) && mShouldCache;
      } catch (IOException ioe) {
        // TODO: Maybe debug log here
        long blockStart = BlockId.getSequenceNumber(currentBlockId) * mBlockSize;
        mCurrentBlockInStream = new UnderStoreFileInStream(blockStart, mBlockSize, mUfsPath);
        mShouldCacheCurrentBlock = mShouldCache;
      }

      // Reading next block entirely
      if (mPos % mBlockSize == 0 && mShouldCacheCurrentBlock) {
        try {
          mCurrentCacheStream =
              mContext.getTachyonBS().getOutStream(currentBlockId, -1, null);
        } catch (IOException ioe) {
          // TODO: Maybe debug log here
          mShouldCacheCurrentBlock = false;
        }
      } else {
        mShouldCacheCurrentBlock = false;
      }
    }
  }
}
