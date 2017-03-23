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
import alluxio.client.BoundedStream;
import alluxio.client.Cancelable;
import alluxio.client.UnderStorageType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.options.CancelUfsFileOptions;
import alluxio.client.file.options.CompleteFileOptions;
import alluxio.client.file.options.CompleteUfsFileOptions;
import alluxio.client.file.options.CreateUfsFileOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
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
  private final UnderFileSystemFileOutStream.Factory mUnderOutStreamFactory;
  private final OutputStream mUnderStorageOutputStream;
  private final OutStreamOptions mOptions;
  /** The client to a file system worker, null if mUfsDelegation is false. */
  private final FileSystemWorkerClient mFileSystemWorkerClient;
  /** The worker file id for the ufs file, null if mUfsDelegation is false. */
  private final Long mUfsFileId;

  private String mUfsPath;

  private boolean mCanceled;
  private boolean mClosed;
  private boolean mShouldCacheCurrentBlock;
  private OutputStream mCurrentBlockOutStream;
  private List<OutputStream> mPreviousBlockOutStreams;

  protected final AlluxioURI mUri;

  /**
   * Creates a new file output stream.
   *
   * @param context the file system context
   * @param path the file path
   * @param options the client options
   * @throws IOException if an I/O error occurs
   */
  public FileOutStream(FileSystemContext context, AlluxioURI path, OutStreamOptions options)
      throws IOException {
    this(path, options, context, UnderFileSystemFileOutStream.Factory.get());
  }

  /**
   * Creates a new file output stream.
   *
   * @param path the file path
   * @param options the client options
   * @param context the file system context
   * @param underOutStreamFactory a factory for creating any necessary under storage out streams
   * @throws IOException if an I/O error occurs
   */
  public FileOutStream(AlluxioURI path, OutStreamOptions options, FileSystemContext context,
      UnderFileSystemFileOutStream.Factory underOutStreamFactory) throws IOException {
    mCloser = Closer.create();
    mUri = Preconditions.checkNotNull(path);
    mBlockSize = options.getBlockSizeBytes();
    mAlluxioStorageType = options.getAlluxioStorageType();
    mUnderStorageType = options.getUnderStorageType();
    mOptions = options;
    mContext = context;
    mBlockStore = AlluxioBlockStore.create(mContext);
    mUnderOutStreamFactory = underOutStreamFactory;
    mPreviousBlockOutStreams = new LinkedList<>();
    mClosed = false;
    mCanceled = false;
    mShouldCacheCurrentBlock = mAlluxioStorageType.isStore();
    mBytesWritten = 0;
    try {
      if (!mUnderStorageType.isSyncPersist()) {
        mUfsPath = null;
        mUnderStorageOutputStream = null;
        mFileSystemWorkerClient = null;
        mUfsFileId = null;
      } else {
        mUfsPath = options.getUfsPath();
        mFileSystemWorkerClient = mCloser.register(mContext.createFileSystemWorkerClient());
        mUfsFileId = mFileSystemWorkerClient.createUfsFile(new AlluxioURI(mUfsPath),
            CreateUfsFileOptions.defaults().setOwner(options.getOwner())
                .setGroup(options.getGroup()).setMode(options.getMode()));
        mUnderStorageOutputStream = mCloser.register(mUnderOutStreamFactory
            .create(mContext, mFileSystemWorkerClient.getWorkerDataServerAddress(), mUfsFileId));
      }
    } catch (AlluxioException | IOException e) {
      mCloser.close();
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw new IOException(e);
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
        mUnderStorageOutputStream.close();
        if (mCanceled) {
          mFileSystemWorkerClient.cancelUfsFile(mUfsFileId, CancelUfsFileOptions.defaults());
        } else {
          long len =
              mFileSystemWorkerClient
                  .completeUfsFile(mUfsFileId, CompleteUfsFileOptions.defaults());
          options.setUfsLength(len);
        }
      }

      if (mAlluxioStorageType.isStore()) {
        if (mCanceled) {
          for (OutputStream bos : mPreviousBlockOutStreams) {
            outStreamCancel(bos);
          }
        } else {
          for (OutputStream bos : mPreviousBlockOutStreams) {
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
    } catch (AlluxioException e) {
      throw mCloser.rethrow(new IOException(e));
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
    if (mShouldCacheCurrentBlock) {
      try {
        if (mCurrentBlockOutStream == null || outStreamRemaining() == 0) {
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

  @Override
  public void write(byte[] b) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_WRITE_BUFFER_NULL);
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_WRITE_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);

    if (mShouldCacheCurrentBlock) {
      try {
        int tLen = len;
        int tOff = off;
        while (tLen > 0) {
          if (mCurrentBlockOutStream == null || outStreamRemaining() == 0) {
            getNextBlock();
          }
          long currentBlockLeftBytes = outStreamRemaining();
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
      Metrics.BYTES_WRITTEN_UFS.inc(len);
    }
    mBytesWritten += len;
  }

  private void getNextBlock() throws IOException {
    if (mCurrentBlockOutStream != null) {
      Preconditions.checkState(outStreamRemaining() <= 0, PreconditionMessage.ERR_BLOCK_REMAINING);
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
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  private void handleCacheWriteException(IOException e) throws IOException {
    LOG.warn("Failed to write into AlluxioStore, canceling write attempt.", e);
    if (!mUnderStorageType.isSyncPersist()) {
      throw new IOException(ExceptionMessage.FAILED_CACHE.getMessage(e.getMessage()), e);
    }

    if (mCurrentBlockOutStream != null) {
      mShouldCacheCurrentBlock = false;
      outStreamCancel(mCurrentBlockOutStream);
    }
  }

  /**
   * Schedules the async persistence of the current file.
   *
   * @throws IOException an I/O error occurs
   */
  protected void scheduleAsyncPersist() throws IOException {
    try (CloseableResource<FileSystemMasterClient> masterClient = mContext
        .acquireMasterClientResource()) {
      masterClient.get().scheduleAsyncPersist(mUri);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  /**
   * @return the remaining bytes in the out stream
   */
  private long outStreamRemaining() {
    assert mCurrentBlockOutStream instanceof BoundedStream;
    return ((BoundedStream) mCurrentBlockOutStream).remaining();
  }

  /**
   * Cancels the out stream.
   *
   * @throws IOException if it fails to cancel the out stream
   */
  private void outStreamCancel(OutputStream outputStream) throws IOException {
    assert outputStream instanceof Cancelable;
    ((Cancelable) outputStream).cancel();
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
