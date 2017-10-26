/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.client.AbstractOutStream;
import alluxio.client.AlluxioStorageType;
import alluxio.client.UnderStorageType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.stream.BlockOutStream;
import alluxio.client.block.stream.UnderFileSystemFileOutStream;
import alluxio.client.file.options.CompleteFileOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.UnavailableException;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

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
  private static final Logger LOG = LoggerFactory.getLogger(FileOutStream.class);

  /** Used to manage closeable resources. */
  private final Closer mCloser;
  private final long mBlockSize;
  private final AlluxioStorageType mAlluxioStorageType;
  private final UnderStorageType mUnderStorageType;
  private final FileSystemContext mContext;
  private final AlluxioBlockStore mBlockStore;
  /** Stream to the file in the under storage, null if not writing to the under storage. */
  private final UnderFileSystemFileOutStream mUnderStorageOutputStream;
  private final OutStreamOptions mOptions;

  private boolean mCanceled;
  private boolean mClosed;
  private boolean mShouldCacheCurrentBlock;
  private BlockOutStream mCurrentBlockOutStream;
  private final List<BlockOutStream> mPreviousBlockOutStreams;

  protected final AlluxioURI mUri;

  /**
   * Creates a new file output stream.
   *
   * @param path the file path
   * @param options the client options
   * @param context the file system context
   */
  public FileOutStream(AlluxioURI path, OutStreamOptions options, FileSystemContext context)
      throws IOException {
    mCloser = Closer.create();
    mUri = Preconditions.checkNotNull(path, "path");
    mBlockSize = options.getBlockSizeBytes();
    mAlluxioStorageType = options.getAlluxioStorageType();
    mUnderStorageType = options.getUnderStorageType();
    mOptions = options;
    mContext = context;
    mBlockStore = AlluxioBlockStore.create(mContext);
    mPreviousBlockOutStreams = new ArrayList<>();
    mClosed = false;
    mCanceled = false;
    mShouldCacheCurrentBlock = mAlluxioStorageType.isStore();
    mBytesWritten = 0;
    if (!mUnderStorageType.isSyncPersist()) {
      mUnderStorageOutputStream = null;
    } else { // Write is through to the under storage, create mUnderStorageOutputStream
      WorkerNetAddress workerNetAddress = // not storing data to Alluxio, so block size is 0
          options.getLocationPolicy().getWorkerForNextBlock(mBlockStore.getWorkerInfoList(), 0);
      if (workerNetAddress == null) {
        // Assume no worker is available because block size is 0
        throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
      }
      try {
        mUnderStorageOutputStream = mCloser
            .register(UnderFileSystemFileOutStream.create(mContext, workerNetAddress, mOptions));
      } catch (Throwable t) {
        throw CommonUtils.closeAndRethrow(mCloser, t);
      }
    }
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
    try {
      if (mCurrentBlockOutStream != null) {
        mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
      }

      CompleteFileOptions options = CompleteFileOptions.defaults();
      if (mUnderStorageType.isSyncPersist()) {
        if (mCanceled) {
          mUnderStorageOutputStream.cancel();
        } else {
          mUnderStorageOutputStream.close();
          options.setUfsLength(mBytesWritten);
        }
      }

      if (mAlluxioStorageType.isStore()) {
        if (mCanceled) {
          for (BlockOutStream bos : mPreviousBlockOutStreams) {
            bos.cancel();
          }
        } else {
          for (BlockOutStream bos : mPreviousBlockOutStreams) {
            bos.close();
          }
        }
      }

      // Complete the file if it's ready to be completed.
      if (!mCanceled && (mUnderStorageType.isSyncPersist() || mAlluxioStorageType.isStore())) {
        try (CloseableResource<FileSystemMasterClient> masterClient = mContext
            .acquireMasterClientResource()) {
          masterClient.get().completeFile(mUri, options);
        }
      }

      if (mUnderStorageType.isAsyncPersist()) {
        scheduleAsyncPersist();
      }
    } catch (Throwable e) { // must catch Throwable
      throw mCloser.rethrow(e); // IOException will be thrown as-is
    } finally {
      mClosed = true;
      mCloser.close();
    }
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
    writeInternal(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_WRITE_BUFFER_NULL);
    writeInternal(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    writeInternal(b, off, len);
  }

  private void writeInternal(int b) throws IOException {
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
      Metrics.BYTES_WRITTEN_UFS.inc();
    }
    mBytesWritten++;
  }

  private void writeInternal(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_WRITE_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);

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
      } catch (Exception e) {
        handleCacheWriteException(e);
      }
    }

    if (mUnderStorageType.isSyncPersist()) {
      mUnderStorageOutputStream.write(b, off, len);
      Metrics.BYTES_WRITTEN_UFS.inc(len);
    }
    mBytesWritten += len;
  }

  private void getNextBlock() throws IOException {
    if (mCurrentBlockOutStream != null) {
      Preconditions.checkState(mCurrentBlockOutStream.remaining() <= 0,
          PreconditionMessage.ERR_BLOCK_REMAINING);
      mCurrentBlockOutStream.flush();
      mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
    }

    if (mAlluxioStorageType.isStore()) {
      mCurrentBlockOutStream =
          mBlockStore.getOutStream(getNextBlockId(), mBlockSize, mOptions);
      mShouldCacheCurrentBlock = true;
    }
  }

  private long getNextBlockId() throws IOException {
    try (CloseableResource<FileSystemMasterClient> masterClient = mContext
        .acquireMasterClientResource()) {
      return masterClient.get().getNewBlockIdForFile(mUri);
    }
  }

  private void handleCacheWriteException(Exception e) throws IOException {
    LOG.warn("Failed to write into AlluxioStore, canceling write attempt.", e);
    if (!mUnderStorageType.isSyncPersist()) {
      throw new IOException(ExceptionMessage.FAILED_CACHE.getMessage(e.getMessage()), e);
    }

    if (mCurrentBlockOutStream != null) {
      mShouldCacheCurrentBlock = false;
      mCurrentBlockOutStream.cancel();
    }
  }

  /**
   * Schedules the async persistence of the current file.
   */
  protected void scheduleAsyncPersist() throws IOException {
    try (CloseableResource<FileSystemMasterClient> masterClient = mContext
        .acquireMasterClientResource()) {
      masterClient.get().scheduleAsyncPersist(mUri);
    }
  }

  /**
   * Class that contains metrics about FileOutStream.
   */
  @ThreadSafe
  private static final class Metrics {
    private static final Counter BYTES_WRITTEN_UFS = MetricsSystem.clientCounter("BytesWrittenUfs");

    private Metrics() {} // prevent instantiation
  }
}
