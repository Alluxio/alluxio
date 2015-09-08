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
import java.io.InputStream;
import java.util.List;

import com.google.common.base.Preconditions;

import tachyon.annotation.PublicApi;
import tachyon.client.BoundedStream;
import tachyon.client.ClientOptions;
import tachyon.client.Seekable;
import tachyon.client.block.BlockInStream;
import tachyon.client.block.BufferedBlockOutStream;
import tachyon.client.block.LocalBlockInStream;
import tachyon.master.block.BlockId;
import tachyon.thrift.FileInfo;

/**
 * Provides a streaming API to read a file. This class wraps the BlockInStreams for each of the
 * blocks in the file and abstracts the switching between streams. The backing streams can read from
 * Tachyon space in the local machine, remote machines, or the under storage system.
 */
@PublicApi
public final class FileInStream extends InputStream implements BoundedStream, Seekable {
  /** Whether the data should be written into Tachyon space */
  private final boolean mShouldCache;
  /** Standard block size in bytes of the file, guaranteed for all but the last block */
  private final long mBlockSize;
  /** Total length of the file in bytes */
  private final long mFileLength;
  /** File System context containing the FileSystemMasterClient pool */
  private final FSContext mContext;
  /** Block ids associated with this file */
  private final List<Long> mBlockIds;
  /** Path to the under storage system file that backs this Tachyon file */
  private final String mUfsPath;

  /** If the stream is closed, this can only go from false to true */
  private boolean mClosed;
  /** Whether or not the current block should be cached. */
  private boolean mShouldCacheCurrentBlock;
  /** Current position of the stream */
  private long mPos;
  /** Current BlockInStream backing this stream */
  private BlockInStream mCurrentBlockInStream;
  /** Current BlockOutStream writing the data into Tachyon, this may be null */
  private BufferedBlockOutStream mCurrentCacheStream;

  /**
   * Creates a new file input stream.
   *
   * @param info the file information
   * @param options the client options
   */
  public FileInStream(FileInfo info, ClientOptions options) {
    mBlockSize = info.getBlockSizeBytes();
    mFileLength = info.getLength();
    mBlockIds = info.getBlockIds();
    mUfsPath = info.getUfsPath();
    mContext = FSContext.INSTANCE;
    mShouldCache = options.getTachyonStorageType().isStore();
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

    int currentOffset = off;
    int bytesLeftToRead = len;

    while (bytesLeftToRead > 0 && mPos < mFileLength) {
      checkAndAdvanceBlockInStream();

      int bytesToRead = (int) Math.min(bytesLeftToRead, mCurrentBlockInStream.remaining());

      int bytesRead = mCurrentBlockInStream.read(b, currentOffset, bytesToRead);
      if (bytesRead > 0 && mShouldCacheCurrentBlock) {
        try {
          mCurrentCacheStream.write(b, currentOffset, bytesRead);
        } catch (IOException ioe) {
          // TODO: Log debug maybe?
          mShouldCacheCurrentBlock = false;
        }
      }
      if (bytesRead == -1) {
        // mCurrentBlockInStream has reached its block boundary
        continue;
      }

      mPos += bytesRead;
      bytesLeftToRead -= bytesRead;
      currentOffset += bytesRead;
    }

    return len - bytesLeftToRead;
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

  /**
   * Convenience method for updating mCurrentBlockInStream, mShouldCacheCurrentBlock, and
   * mCurrentCacheStream. If the block boundary has been reached, the current BlockInStream is
   * closed and a the next one is opened. mShouldCacheCurrent block is reset to the original
   * mShouldCache. mCurrentCacheStream is also closed and a new one is created for the next block.
   *
   * @throws IOException if the next BlockInStream cannot be obtained
   */
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
          // TODO: Specify the location to be local
          mCurrentCacheStream =
              mContext.getTachyonBS().getOutStream(currentBlockId, -1, null);
        } catch (IOException ioe) {
          // TODO: Maybe debug log here
          mShouldCacheCurrentBlock = false;
        }
      }
    }
  }

  /**
   * Convenience method for checking if mCurrentCacheStream is not null and closing it with the
   * appropriate close or cancel command.
   *
   * @throws IOException if the close fails
   */
  private void closeCacheStream() throws IOException {
    if (mCurrentCacheStream == null) {
      return;
    }
    if (mCurrentCacheStream.remaining() == 0) {
      mCurrentCacheStream.close();
    } else {
      mCurrentCacheStream.cancel();
    }
    mShouldCacheCurrentBlock = false;
  }

  /**
   * @return the current block id based on mPos
   */
  private long getBlockCurrentBlockId() {
    int index = (int) (mPos / mBlockSize);
    Preconditions.checkState(index < mBlockIds.size(), "Current block index exceeds max index.");
    return mBlockIds.get(index);
  }

  /**
   * Similar to checkAndAdvanceBlockInStream, but a specific position can be specified and the
   * stream pointer will be at that offset after this method completes.
   *
   * @param newPos the new position to set the stream to
   * @throws IOException if the stream at the specified position cannot be opened
   */
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
