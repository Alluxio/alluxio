/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.client.AbstractOutStream;
import alluxio.client.AlluxioStorageType;
import alluxio.client.ClientContext;
import alluxio.client.UnderStorageType;
import alluxio.client.block.BufferedBlockOutStream;
import alluxio.client.file.options.CompleteFileOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a streaming API to write a file. This class wraps the BlockOutStreams for each of the
 * blocks in the file and abstracts the switching between streams. The backing streams can write to
 * Alluxio space in the local machine or remote machines. If the {@link UnderStorageType} is
 * {@link UnderStorageType#SYNC_PERSIST}, another stream will write the data to the under storage
 * system.
 */
@PublicApi
@NotThreadSafe
public class FileOutStream extends AbstractOutStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final long mBlockSize;
  protected final AlluxioStorageType mAlluxioStorageType;
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

  protected final AlluxioURI mUri;

  /**
   * Creates a new file output stream.
   *
   * @param path the file path
   * @param options the client options
   * @throws IOException if an I/O error occurs
   */
  public FileOutStream(AlluxioURI path, OutStreamOptions options) throws IOException {
    mUri = Preconditions.checkNotNull(path);
    mNonce = IdUtils.getRandomNonNegativeLong();
    mBlockSize = options.getBlockSizeBytes();
    mAlluxioStorageType = options.getAlluxioStorageType();
    mUnderStorageType = options.getUnderStorageType();
    mContext = FileSystemContext.INSTANCE;
    mPreviousBlockOutStreams = new LinkedList<BufferedBlockOutStream>();
    if (mUnderStorageType.isSyncPersist()) {
      updateUfsPath();
      String tmpPath = PathUtils.temporaryFileName(mNonce, mUfsPath);
      UnderFileSystem ufs = UnderFileSystem.get(tmpPath, ClientContext.getConf());
      // TODO(jiri): Implement collection of temporary files left behind by dead clients.
      mUnderStorageOutputStream = ufs.create(tmpPath, (int) mBlockSize);
    } else {
      mUfsPath = null;
      mUnderStorageOutputStream = null;
    }
    mClosed = false;
    mCanceled = false;
    mShouldCacheCurrentBlock = mAlluxioStorageType.isStore();
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

    CompleteFileOptions options = CompleteFileOptions.defaults();
    if (mUnderStorageType.isSyncPersist()) {
      String tmpPath = PathUtils.temporaryFileName(mNonce, mUfsPath);
      UnderFileSystem ufs = UnderFileSystem.get(tmpPath, ClientContext.getConf());
      if (mCanceled) {
        // TODO(yupeng): Handle this special case in under storage integrations.
        mUnderStorageOutputStream.close();
        if (!ufs.exists(tmpPath)) {
          // Location of the temporary file has changed, recompute it.
          updateUfsPath();
          tmpPath = PathUtils.temporaryFileName(mNonce, mUfsPath);
        }
        ufs.delete(tmpPath, false);
      } else {
        mUnderStorageOutputStream.flush();
        mUnderStorageOutputStream.close();
        if (!ufs.exists(tmpPath)) {
          // Location of the temporary file has changed, recompute it.
          updateUfsPath();
          tmpPath = PathUtils.temporaryFileName(mNonce, mUfsPath);
        }
        if (!ufs.rename(tmpPath, mUfsPath)) {
          throw new IOException("Failed to rename " + tmpPath + " to " + mUfsPath);
        }
        options.setUfsLength(ufs.getFileSize(mUfsPath));
      }
    }

    if (mAlluxioStorageType.isStore()) {
      try {
        if (mCanceled) {
          for (BufferedBlockOutStream bos : mPreviousBlockOutStreams) {
            bos.cancel();
          }
        } else {
          for (BufferedBlockOutStream bos : mPreviousBlockOutStreams) {
            bos.close();
          }
        }
      } catch (IOException e) {
        handleCacheWriteException(e);
      }
    }

    // Complete the file if it's ready to be completed.
    if (!mCanceled && (mUnderStorageType.isSyncPersist() || mAlluxioStorageType.isStore())) {
      FileSystemMasterClient masterClient = mContext.acquireMasterClient();
      try {
        masterClient.completeFile(mUri, options);
      } catch (AlluxioException e) {
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
    // TODO(yupeng): Handle flush for Alluxio storage stream as well.
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
      } catch (IOException e) {
        handleCacheWriteException(e);
      }
    }

    if (mUnderStorageType.isSyncPersist()) {
      mUnderStorageOutputStream.write(b);
      ClientContext.getClientMetrics().incBytesWrittenUfs(1);
    }
    mBytesWritten++;
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
            tLen = 0;
          } else {
            mCurrentBlockOutStream.write(b, tOff, (int) currentBlockLeftBytes);
            tOff += currentBlockLeftBytes;
            tLen -= currentBlockLeftBytes;
          }
        }
      } catch (IOException e) {
        handleCacheWriteException(e);
      }
    }

    if (mUnderStorageType.isSyncPersist()) {
      mUnderStorageOutputStream.write(b, off, len);
      ClientContext.getClientMetrics().incBytesWrittenUfs(len);
    }
    mBytesWritten += len;
  }

  private void getNextBlock() throws IOException {
    if (mCurrentBlockOutStream != null) {
      Preconditions.checkState(mCurrentBlockOutStream.remaining() <= 0,
          PreconditionMessage.ERR_BLOCK_REMAINING);
      mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
    }

    if (mAlluxioStorageType.isStore()) {
      try {
        WorkerNetAddress address = mLocationPolicy
            .getWorkerForNextBlock(mContext.getAlluxioBlockStore().getWorkerInfoList(), mBlockSize);
        mCurrentBlockOutStream =
            mContext.getAlluxioBlockStore().getOutStream(getNextBlockId(), mBlockSize, address);
        mShouldCacheCurrentBlock = true;
      } catch (AlluxioException e) {
        throw new IOException(e);
      }
    }
  }

  private long getNextBlockId() throws IOException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.getNewBlockIdForFile(mUri);
    } catch (AlluxioException e) {
      throw new IOException(e);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  protected void handleCacheWriteException(IOException e) throws IOException {
    if (!mUnderStorageType.isSyncPersist()) {
      throw new IOException(ExceptionMessage.FAILED_CACHE.getMessage(e.getMessage()), e);
    }

    LOG.warn("Failed to write into AlluxioStore, canceling write attempt.", e);
    if (mCurrentBlockOutStream != null) {
      mShouldCacheCurrentBlock = false;
      mCurrentBlockOutStream.cancel();
    }
  }

  private void updateUfsPath() throws IOException {
    FileSystemMasterClient client = mContext.acquireMasterClient();
    try {
      URIStatus status = client.getStatus(mUri);
      mUfsPath = status.getUfsPath();
    } catch (AlluxioException e) {
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
      masterClient.scheduleAsyncPersist(mUri);
    } catch (AlluxioException e) {
      throw new IOException(e);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }
}
