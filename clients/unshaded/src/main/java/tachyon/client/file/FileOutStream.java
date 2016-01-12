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
import tachyon.annotation.PublicApi;
import tachyon.client.AbstractOutStream;
import tachyon.client.ClientContext;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.Utils;
import tachyon.client.block.BufferedBlockOutStream;
import tachyon.client.file.options.CompleteFileOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.client.file.policy.FileWriteLocationPolicy;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.PreconditionMessage;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.PathUtils;
import tachyon.worker.NetAddress;

/**
 * Provides a streaming API to write a file. This class wraps the BlockOutStreams for each of the
 * blocks in the file and abstracts the switching between streams. The backing streams can write to
 * Tachyon space in the local machine or remote machines. If the {@link UnderStorageType} is
 * {@link UnderStorageType#SYNC_PERSIST}, another stream will write the data to the under storage
 * system.
 */
@PublicApi
public class FileOutStream extends AbstractOutStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final long mBlockSize;
  protected final TachyonStorageType mTachyonStorageType;
  private final UnderStorageType mUnderStorageType;
  private final FileSystemContext mContext;
  private final OutputStream mUnderStorageOutputStream;
  private final long mNonce;
  private String mUfsPath;
  private FileWriteLocationPolicy mLocationPolicy;

  protected boolean mCanceled;
  protected boolean mClosed;
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
    mNonce = Utils.getRandomNonNegativeLong();
    mBlockSize = options.getBlockSizeBytes();
    mTachyonStorageType = options.getTachyonStorageType();
    mUnderStorageType = options.getUnderStorageType();
    mContext = FileSystemContext.INSTANCE;
    mPreviousBlockOutStreams = new LinkedList<BufferedBlockOutStream>();
    if (mUnderStorageType.isSyncPersist()) {
      updateUfsPath();
      String tmpPath = PathUtils.temporaryFileName(fileId, mNonce, mUfsPath);
      UnderFileSystem ufs = UnderFileSystem.get(tmpPath, ClientContext.getConf());
      // TODO(jiri): Implement collection of temporary files left behind by dead clients.
      mUnderStorageOutputStream = ufs.create(tmpPath, (int) mBlockSize);
    } else {
      mUfsPath = null;
      mUnderStorageOutputStream = null;
    }
    mClosed = false;
    mCanceled = false;
    mShouldCacheCurrentBlock = mTachyonStorageType.isStore();
    mBytesWritten = 0;
    mLocationPolicy = Preconditions.checkNotNull(options.getLocationPolicy(),
        PreconditionMessage.FILE_WRITE_LOCATION_POLICY_UNSPECIFIED);
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
    CompleteFileOptions.Builder builder = new CompleteFileOptions.Builder(ClientContext.getConf());
    if (mUnderStorageType.isSyncPersist()) {
      String tmpPath = PathUtils.temporaryFileName(mFileId, mNonce, mUfsPath);
      UnderFileSystem ufs = UnderFileSystem.get(tmpPath, ClientContext.getConf());
      if (mCanceled) {
        // TODO(yupeng): Handle this special case in under storage integrations.
        mUnderStorageOutputStream.close();
        if (!ufs.exists(tmpPath)) {
          // Location of the temporary file has changed, recompute it.
          updateUfsPath();
          tmpPath = PathUtils.temporaryFileName(mFileId, mNonce, mUfsPath);
        }
        ufs.delete(tmpPath, false);
      } else {
        mUnderStorageOutputStream.flush();
        mUnderStorageOutputStream.close();
        if (!ufs.exists(tmpPath)) {
          // Location of the temporary file has changed, recompute it.
          updateUfsPath();
          tmpPath = PathUtils.temporaryFileName(mFileId, mNonce, mUfsPath);
        }
        if (!ufs.rename(tmpPath, mUfsPath)) {
          throw new IOException("Failed to rename " + tmpPath + " to " + mUfsPath);
        }
        builder.setUfsLength(ufs.getFileSize(mUfsPath));
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
        masterClient.completeFile(mFileId, builder.build());
      } catch (TachyonException e) {
        throw new IOException(e);
      } finally {
        mContext.releaseMasterClient(masterClient);
      }
    }

    if (mUnderStorageType.isAsyncPersist()) {
      scheduleAsyncPersist();
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
        mBytesWritten++;
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
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_WRITE_BUFFER_NULL);
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_WRITE_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE, b.length, off, len);

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
            mBytesWritten += tLen;
            tLen = 0;
          } else {
            mCurrentBlockOutStream.write(b, tOff, (int) currentBlockLeftBytes);
            mBytesWritten += (int) currentBlockLeftBytes;
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
      Preconditions.checkState(mCurrentBlockOutStream.remaining() <= 0,
          PreconditionMessage.ERR_BLOCK_REMAINING);
      mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
    }

    if (mTachyonStorageType.isStore()) {
      try {
        NetAddress address = mLocationPolicy.getWorkerForNextBlock(
            mContext.getTachyonBlockStore().getWorkerInfoList(), mBlockSize);
        mCurrentBlockOutStream =
            mContext.getTachyonBlockStore().getOutStream(getNextBlockId(), mBlockSize, address);
        mShouldCacheCurrentBlock = true;
      } catch (TachyonException e) {
        throw new IOException(e);
      }
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
      throw new IOException(ExceptionMessage.FAILED_CACHE.getMessage(ioe.getMessage()), ioe);
    }

    LOG.warn("Failed to write into TachyonStore, canceling write attempt.", ioe);
    if (mCurrentBlockOutStream != null) {
      mShouldCacheCurrentBlock = false;
      mCurrentBlockOutStream.cancel();
    }
  }

  private void updateUfsPath() throws IOException {
    FileSystemMasterClient client = mContext.acquireMasterClient();
    try {
      FileInfo fileInfo = client.getFileInfo(mFileId);
      mUfsPath = fileInfo.getUfsPath();
    } catch (TachyonException e) {
      throw new IOException(e);
    } finally {
      mContext.releaseMasterClient(client);
    }
  }

  /**
   * Schedules the async persistence of the current file.
   *
   * @throws IOException an I/O error occurs
   */
  protected void scheduleAsyncPersist() throws IOException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.scheduleAsyncPersist(mFileId);
    } catch (TachyonException e) {
      throw new IOException(e);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }
}
