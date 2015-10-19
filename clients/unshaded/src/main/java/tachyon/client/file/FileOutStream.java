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
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.Cancelable;
import tachyon.client.ClientContext;
import tachyon.client.FileSystemMasterClient;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.block.BlockStoreContext;
import tachyon.client.block.BufferedBlockOutStream;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.PathUtils;
import tachyon.worker.WorkerClient;

/**
 * Provides a streaming API to write a file. This class wraps the BlockOutStreams for each of the
 * blocks in the file and abstracts the switching between streams. The backing streams can write to
 * Tachyon space in the local machine or remote machines. If the
 * {@link tachyon.client.UnderStorageType} is SYNC_PERSIST, another stream will write the data to
 * the under storage system.
 */
@PublicApi
public class FileOutStream extends OutputStream implements Cancelable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  // Error strings for preconditions in order to improve performance
  private static final String ERR_BLOCK_REMAINING =
      "The current block still has space left, no need to get new block.";
  private static final String ERR_BUFFER_NULL = "Cannot write a null input buffer.";
  private static final String ERR_BUFFER_STATE = "Buffer length: %s, offset: %s, len: %s";

  private final long mBlockSize;
  protected final TachyonStorageType mTachyonStorageType;
  private final UnderStorageType mUnderStorageType;
  private final FileSystemContext mContext;
  private final OutputStream mUnderStorageOutputStream;
  private final long mNonce;
  private String mUfsPath;

  protected boolean mCanceled;
  protected boolean mClosed;
  private String mHostname;
  private boolean mShouldCacheCurrentBlock;
  protected BufferedBlockOutStream mCurrentBlockOutStream;
  protected List<BufferedBlockOutStream> mPreviousBlockOutStreams;

  protected final long mFileId;

  /**
   * Creates a new file output stream.
   *
   * @param fileId the file id
   * @param options the client options
   * @throws IOException if an I/O error occurs
   */
  public FileOutStream(long fileId, OutStreamOptions options) throws IOException {
    mFileId = fileId;
    mNonce = ClientContext.getRandomNonNegativeLong();
    mBlockSize = options.getBlockSize();
    mTachyonStorageType = options.getTachyonStorageType();
    mUnderStorageType = options.getUnderStorageType();
    mContext = FileSystemContext.INSTANCE;
    mPreviousBlockOutStreams = new LinkedList<BufferedBlockOutStream>();
    if (mUnderStorageType.isSyncPersist()) {
      FileInfo fileInfo = getFileInfo();
      mUfsPath = fileInfo.getUfsPath();
      String fileName = PathUtils.temporaryFileName(fileId, mNonce, mUfsPath);
      UnderFileSystem ufs = UnderFileSystem.get(fileName, ClientContext.getConf());
      String parentPath = (new TachyonURI(mUfsPath)).getParent().getPath();
      if (!ufs.exists(parentPath) && !ufs.mkdirs(parentPath, true)) {
        throw new IOException("Failed to create " + parentPath);
      }
      // TODO(jiri): Implement collection of temporary files left behind by dead clients.
      mUnderStorageOutputStream = ufs.create(fileName, (int) mBlockSize);
    } else {
      mUfsPath = null;
      mUnderStorageOutputStream = null;
    }
    mClosed = false;
    mCanceled = false;
    mHostname = options.getHostname();
    mShouldCacheCurrentBlock = mTachyonStorageType.isStore();
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
    if (mUnderStorageType.isSyncPersist()) {
      if (mCanceled) {
        // TODO(yupeng): Handle this special case in under storage integrations.
        mUnderStorageOutputStream.close();
        String tmpPath = PathUtils.temporaryFileName(mFileId, mNonce, mUfsPath);
        UnderFileSystem ufs = UnderFileSystem.get(tmpPath, ClientContext.getConf());
        if (!ufs.exists(tmpPath)) {
          FileInfo fileInfo = getFileInfo();
          mUfsPath = fileInfo.getUfsPath();
          tmpPath = PathUtils.temporaryFileName(mFileId, mNonce, mUfsPath);
        }
        ufs.delete(tmpPath, false);
      } else {
        mUnderStorageOutputStream.flush();
        mUnderStorageOutputStream.close();
        WorkerClient workerClient = BlockStoreContext.INSTANCE.acquireWorkerClient();
        try {
          // TODO(yupeng): Investigate if this RPC can be moved to master.
          workerClient.persistFile(mFileId, mNonce, mUfsPath);
        } finally {
          BlockStoreContext.INSTANCE.releaseWorkerClient(workerClient);
        }
        canComplete = true;
      }
    }

    if (mTachyonStorageType.isStore()) {
      try {
        if (mCanceled) {
          for (BufferedBlockOutStream bos : mPreviousBlockOutStreams) {
            bos.cancel();
          }
        } else {
          for (BufferedBlockOutStream bos : mPreviousBlockOutStreams) {
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
      } catch (TachyonException e) {
        throw new IOException(e);
      } finally {
        mContext.releaseMasterClient(masterClient);
      }
    }
    mClosed = true;
  }

  @Override
  public void flush() throws IOException {
    // TODO(yupeng): Handle flush for Tachyon storage stream as well.
    if (mUnderStorageType.isSyncPersist()) {
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
      } catch (IOException ioe) {
        handleCacheWriteException(ioe);
      }
    }

    if (mUnderStorageType.isSyncPersist()) {
      mUnderStorageOutputStream.write(b);
      ClientContext.getClientMetrics().incBytesWrittenUfs(1);
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, ERR_BUFFER_NULL);
    Preconditions.checkArgument(
        off >= 0 && len >= 0 && len + off <= b.length, ERR_BUFFER_STATE, b.length, off, len);

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
            tLen = 0;
          } else {
            mCurrentBlockOutStream.write(b, tOff, (int) currentBlockLeftBytes);
            tOff += currentBlockLeftBytes;
            tLen -= currentBlockLeftBytes;
          }
        }
      } catch (IOException ioe) {
        handleCacheWriteException(ioe);
      }
    }

    if (mUnderStorageType.isSyncPersist()) {
      mUnderStorageOutputStream.write(b, off, len);
      ClientContext.getClientMetrics().incBytesWrittenUfs(len);
    }
  }

  private void getNextBlock() throws IOException {
    if (mCurrentBlockOutStream != null) {
      Preconditions.checkState(mCurrentBlockOutStream.remaining() <= 0, ERR_BLOCK_REMAINING);
      mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
    }

    if (mTachyonStorageType.isStore()) {
      mCurrentBlockOutStream =
          mContext.getTachyonBlockStore().getOutStream(getNextBlockId(), mBlockSize, mHostname);
      mShouldCacheCurrentBlock = true;
    }
  }

  private long getNextBlockId() throws IOException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.getNewBlockIdForFile(mFileId);
    } catch (TachyonException e) {
      throw new IOException(e);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  protected void handleCacheWriteException(IOException ioe) throws IOException {
    if (!mUnderStorageType.isSyncPersist()) {
      // TODO(yupeng): Handle this exception better.
      throw new IOException("Fail to cache: " + ioe.getMessage(), ioe);
    }

    LOG.warn("Failed to write into TachyonStore, canceling write attempt.", ioe);
    if (mCurrentBlockOutStream != null) {
      mShouldCacheCurrentBlock = false;
      mCurrentBlockOutStream.cancel();
    }
  }

  private FileInfo getFileInfo() throws IOException {
    FileSystemMasterClient client = mContext.acquireMasterClient();
    try {
      return client.getFileInfo(mFileId);
    } catch (TachyonException   e) {
      throw new IOException(e.getMessage());
    } finally {
      mContext.releaseMasterClient(client);
    }
  }
}
