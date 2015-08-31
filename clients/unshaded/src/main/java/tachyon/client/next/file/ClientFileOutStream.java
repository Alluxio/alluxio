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

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Preconditions;

import tachyon.client.FileSystemMasterClient;
import tachyon.client.next.CacheType;
import tachyon.client.next.ClientContext;
import tachyon.client.next.ClientOptions;
import tachyon.client.next.UnderStorageType;
import tachyon.client.next.block.BSContext;
import tachyon.client.next.block.BlockOutStream;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.PathUtils;
import tachyon.worker.next.WorkerClient;

/**
 * Provides a streaming API to write a file. This class wraps the BlockOutStreams for each of the
 * blocks in the file and abstracts the switching between streams. The backing streams can write to
 * Tachyon space in the local machine or remote machines. If the
 * {@link tachyon.client.next.UnderStorageType} is PERSIST, another stream will write the data to
 * the under storage system.
 */
public class ClientFileOutStream extends FileOutStream {
  private final long mFileId;
  private final ClientOptions mOptions;
  private final long mBlockSize;
  private final CacheType mCacheType;
  private final UnderStorageType mUnderStorageType;
  private final FSContext mContext;
  private final OutputStream mUnderStorageOutputStream;
  private final String mUnderStorageFile;
  private final WorkerClient mWorkerClient;

  private boolean mCanceled;
  private boolean mClosed;
  private boolean mShouldCacheCurrentBlock;
  private long mCachedBytes;
  private BlockOutStream mCurrentBlockOutStream;
  private List<BlockOutStream> mPreviousBlockOutStreams;

  public ClientFileOutStream(long fileId, ClientOptions options) throws IOException {
    mFileId = fileId;
    mOptions = options;
    mBlockSize = options.getBlockSize();
    mCacheType = options.getCacheType();
    mUnderStorageType = options.getUnderStorageType();
    mContext = FSContext.INSTANCE;
    mPreviousBlockOutStreams = new LinkedList<BlockOutStream>();
    if (mUnderStorageType.shouldPersist()) {
      mWorkerClient = BSContext.INSTANCE.acquireWorkerClient();
      String userUnderStorageFolder = mWorkerClient.getUserUfsTempFolder();
      mUnderStorageFile = PathUtils.concatPath(userUnderStorageFolder, mFileId);
      UnderFileSystem underStorageClient =
          UnderFileSystem.get(mUnderStorageFile, ClientContext.getConf());
      underStorageClient.mkdirs(userUnderStorageFolder, true);
      mUnderStorageOutputStream = underStorageClient.create(mUnderStorageFile, (int) mBlockSize);
    } else {
      mWorkerClient = null;
      mUnderStorageFile = null;
      mUnderStorageOutputStream = null;
    }
    mClosed = false;
    mCanceled = false;
    mShouldCacheCurrentBlock = mCacheType.shouldCache();
  }

  @Override
  public void cancel() throws IOException {
    mCanceled = true;
    close();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    if (mCurrentBlockOutStream != null) {
      mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
    }

    Boolean canComplete = false;
    if (mUnderStorageType.shouldPersist()) {
      if (mCanceled) {
        // TODO: Handle this special case in under storage integrations
        mUnderStorageOutputStream.close();
        UnderFileSystem underFsClient =
            UnderFileSystem.get(mUnderStorageFile, ClientContext.getConf());
        underFsClient.delete(mUnderStorageFile, false);
      } else {
        mUnderStorageOutputStream.flush();
        mUnderStorageOutputStream.close();
        try {
          // TODO: Investigate if this RPC can be moved to master
          // TODO: Make this RPC take a long
          mWorkerClient.addCheckpoint((int) mFileId);
        } finally {
          BSContext.INSTANCE.releaseWorkerClient(mWorkerClient);
        }
        canComplete = true;
      }
    }

    if (mCacheType.shouldCache()) {
      try {
        if (mCanceled) {
          for (BlockOutStream bos : mPreviousBlockOutStreams) {
            bos.cancel();
          }
        } else {
          for (BlockOutStream bos : mPreviousBlockOutStreams) {
            bos.close();
          }
          canComplete = true;
        }
      } catch (IOException ioe) {
        handleCacheWriteException(ioe);
      }
    }

    if (canComplete) {
      FileSystemMasterClient masterClient = mContext.acquireMasterClient();
      try {
        masterClient.completeFile(mFileId);
      } finally {
        mContext.releaseMasterClient(masterClient);
      }
    }
    mClosed = true;
  }

  @Override
  public void flush() throws IOException {
    // TODO: Handle flush for Tachyon storage stream as well
    if (mUnderStorageType.shouldPersist()) {
      mUnderStorageOutputStream.flush();
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (mShouldCacheCurrentBlock) {
      try {
        if (mCurrentBlockOutStream == null || mCurrentBlockOutStream.remaining() == 0) {
          getNextBlock();
        }
        mCurrentBlockOutStream.write(b);
        mCachedBytes ++;
      } catch (IOException ioe) {
        handleCacheWriteException(ioe);
      }
    }

    if (mUnderStorageType.shouldPersist()) {
      mUnderStorageOutputStream.write(b);
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, "Buffer is null");
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length, String
        .format("Buffer length (%d), offset(%d), len(%d)", b.length, off, len));

    if (mShouldCacheCurrentBlock) {
      try {
        int tLen = len;
        int tOff = off;
        while (tLen > 0) {
          if (mCurrentBlockOutStream == null || mCurrentBlockOutStream.remaining() == 0) {
            getNextBlock();
          }
          long currentBlockLeftBytes = mCurrentBlockOutStream.remaining();
          if (currentBlockLeftBytes >= tLen) {
            mCurrentBlockOutStream.write(b, tOff, tLen);
            mCachedBytes += tLen;
            tLen = 0;
          } else {
            mCurrentBlockOutStream.write(b, tOff, (int) currentBlockLeftBytes);
            tOff += currentBlockLeftBytes;
            tLen -= currentBlockLeftBytes;
            mCachedBytes += currentBlockLeftBytes;
          }
        }
      } catch (IOException ioe) {
        handleCacheWriteException(ioe);
      }
    }

    if (mUnderStorageType.shouldPersist()) {
      mUnderStorageOutputStream.write(b, off, len);
    }
  }

  private void getNextBlock() throws IOException {
    if (mCurrentBlockOutStream != null) {
      Preconditions.checkState(mCurrentBlockOutStream.remaining() <= 0, "The current block still "
          + "has space left, no need to get new block");
      mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
    }

    if (mCacheType.shouldCache()) {
      mCurrentBlockOutStream = mContext.getTachyonBS().getOutStream(getNextBlockId(), mOptions);
      mShouldCacheCurrentBlock = true;
    }
  }

  private long getNextBlockId() throws IOException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.getNewBlockIdForFile(mFileId);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  private void handleCacheWriteException(IOException ioe) throws IOException {
    if (!mUnderStorageType.shouldPersist()) {
      // TODO: Handle this exception better
      throw new IOException("Fail to cache: " + ioe.getMessage(), ioe);
    } else {
      // TODO: Handle this error
      if (mCurrentBlockOutStream != null) {
        mShouldCacheCurrentBlock = false;
        mCurrentBlockOutStream.cancel();
      }
    }
  }
}
