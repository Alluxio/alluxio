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

import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.client.AlluxioStorageType;
import alluxio.client.BoundedStream;
import alluxio.client.Seekable;
import alluxio.client.block.BlockInStream;
import alluxio.client.block.BufferedBlockOutStream;
import alluxio.client.block.LocalBlockInStream;
import alluxio.client.block.RemoteBlockInStream;
import alluxio.client.block.UnderStoreBlockInStream;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.master.block.BlockId;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A streaming API to read a file. This API represents a file as a stream of bytes and provides a
 * collection of {@link #read} methods to access this stream of bytes. In addition, one can seek
 * into a given offset of the stream to read.
 * <p>
 * This class wraps the {@link BlockInStream} for each of the blocks in the file and abstracts the
 * switching between streams. The backing streams can read from Alluxio space in the local machine,
 * remote machines, or the under storage system.
 */
@PublicApi
@NotThreadSafe
public class FileInStream extends InputStream implements BoundedStream, Seekable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** How the data should be written into Alluxio space, if at all. */
  private final AlluxioStorageType mAlluxioStorageType;
  /** Standard block size in bytes of the file, guaranteed for all but the last block. */
  private final long mBlockSize;
  /** The location policy for CACHE type of read into Alluxio. */
  private final FileWriteLocationPolicy mLocationPolicy;
  /** Total length of the file in bytes. */
  private final long mFileLength;
  /** File System context containing the {@link FileSystemMasterClient} pool. */
  private final FileSystemContext mContext;
  /** File information. */
  private final URIStatus mStatus;
  /** Constant error message for block ID not cached. */
  private static final String BLOCK_ID_NOT_CACHED =
      "The block with ID {} could not be cached into Alluxio storage.";
  /** Error message for cache collision. */
  private static final String BLOCK_ID_EXISTS_SO_NOT_CACHED =
      "The block with ID {} is already stored in the target worker, canceling the cache request.";

  /** If the stream is closed, this can only go from false to true. */
  private boolean mClosed;
  /** Whether or not the current block should be cached. */
  private boolean mShouldCacheCurrentBlock;
  /** Current position of the stream. */
  private long mPos;
  /** Current {@link BlockInStream} backing this stream. */
  private BlockInStream mCurrentBlockInStream;
  /** Current {@link BufferedBlockOutStream} writing the data into Alluxio, this may be null. */
  private BufferedBlockOutStream mCurrentCacheStream;

  /**
   * Creates a new file input stream.
   *
   * @param status the file status
   * @param options the client options
   */
  public FileInStream(URIStatus status, InStreamOptions options) {
    mStatus = status;
    mBlockSize = status.getBlockSizeBytes();
    mFileLength = status.getLength();
    mContext = FileSystemContext.INSTANCE;
    mAlluxioStorageType = options.getAlluxioStorageType();
    mShouldCacheCurrentBlock = mAlluxioStorageType.isStore();
    mClosed = false;
    mLocationPolicy = options.getLocationPolicy();
    if (mShouldCacheCurrentBlock) {
      Preconditions.checkNotNull(options.getLocationPolicy(),
          PreconditionMessage.FILE_WRITE_LOCATION_POLICY_UNSPECIFIED);
    }
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
    mPos++;
    if (mShouldCacheCurrentBlock) {
      try {
        mCurrentCacheStream.write(data);
      } catch (IOException e) {
        logCacheStreamIOException(e);
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
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE, b.length, off, len);
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
        } catch (IOException e) {
          logCacheStreamIOException(e);
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
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE, pos);
    Preconditions.checkArgument(pos < mFileLength, PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE,
        pos);

    seekBlockInStream(pos);
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
    long toSkipInBlock = ((newPos / mBlockSize) > mPos / mBlockSize) ? newPos % mBlockSize : toSkip;
    seekBlockInStream(newPos);
    checkAndAdvanceBlockInStream();
    if (toSkipInBlock != mCurrentBlockInStream.skip(toSkipInBlock)) {
      throw new IOException(ExceptionMessage.INSTREAM_CANNOT_SKIP.getMessage(toSkip));
    }
    return toSkip;
  }

  /**
   * Convenience method for updating {@link #mCurrentBlockInStream},
   * {@link #mShouldCacheCurrentBlock}, and {@link #mCurrentCacheStream}. If the block boundary has
   * been reached, the current {@link BlockInStream} is closed and a the next one is opened.
   * {@link #mShouldCacheCurrentBlock} is set to {@link #mAlluxioStorageType}.isCache().
   * {@link #mCurrentCacheStream} is also closed and a new one is created for the next block.
   *
   * @throws IOException if the next BlockInStream cannot be obtained
   */
  private void checkAndAdvanceBlockInStream() throws IOException {
    long currentBlockId = getCurrentBlockId();
    if (mCurrentBlockInStream == null || mCurrentBlockInStream.remaining() == 0) {
      closeCacheStream();
      updateBlockInStream(currentBlockId);
      if (mShouldCacheCurrentBlock) {
        try {
          long blockSize = getCurrentBlockSize();
          WorkerNetAddress address = mLocationPolicy.getWorkerForNextBlock(
              mContext.getAlluxioBlockStore().getWorkerInfoList(), blockSize);
          // Don't cache the block to somewhere that already has it.
          // TODO(andrew): Filter the workers provided to the location policy to not include
          // workers which already contain the block. See ALLUXIO-1816.
          if (mCurrentBlockInStream instanceof RemoteBlockInStream) {
            WorkerNetAddress readAddress =
                ((RemoteBlockInStream) mCurrentBlockInStream).getWorkerNetAddress();
            // Try to avoid an RPC.
            if (readAddress.equals(address)) {
              mShouldCacheCurrentBlock = false;
            } else {
              BlockInfo blockInfo = mContext.getAlluxioBlockStore().getInfo(currentBlockId);
              for (BlockLocation location : blockInfo.getLocations()) {
                if (address.equals(location.getWorkerAddress())) {
                  mShouldCacheCurrentBlock = false;
                }
              }
            }
          }
          if (mShouldCacheCurrentBlock) {
            mCurrentCacheStream =
                mContext.getAlluxioBlockStore().getOutStream(currentBlockId, blockSize, address);
          }
        } catch (IOException e) {
          logCacheStreamIOException(e);
          mShouldCacheCurrentBlock = false;
        } catch (AlluxioException e) {
          LOG.warn(BLOCK_ID_NOT_CACHED, currentBlockId, e);
          mShouldCacheCurrentBlock = false;
        }
      }
    }
  }

  /**
   * Convenience method for checking if {@link #mCurrentCacheStream} is not null and closing it
   * with the appropriate close or cancel command.
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
   * @return the current block id based on mPos, -1 if at the end of the file
   */
  private long getCurrentBlockId() {
    if (mPos == mFileLength) {
      return -1;
    }
    int index = (int) (mPos / mBlockSize);
    Preconditions.checkState(index < mStatus.getBlockIds().size(),
        PreconditionMessage.ERR_BLOCK_INDEX);
    return mStatus.getBlockIds().get(index);
  }

  /**
   * @return the size of the current block being written
   */
  private long getCurrentBlockSize() {
    // The size of the last block, 0 if it is equal to the normal block size
    long lastBlockSize = mFileLength % mBlockSize;
    // If we are not in the last block or if the last block is equal to the normal block size,
    // return the normal block size. Otherwise return the block size of the last block.
    if (mFileLength - mPos > lastBlockSize) {
      return mBlockSize;
    } else {
      return lastBlockSize;
    }
  }

  /**
   * Logs IO exceptions thrown in response to the worker cache request. If the exception is not an
   * expected exception, a warning will be logged with the stack trace.
   */
  private void logCacheStreamIOException(IOException e) {
    if (e.getCause() instanceof BlockAlreadyExistsException) {
      LOG.warn(BLOCK_ID_EXISTS_SO_NOT_CACHED, getCurrentBlockId());
    } else {
      LOG.warn(BLOCK_ID_NOT_CACHED, getCurrentBlockId(), e);
    }
  }

  /**
   * Similar to {@link #checkAndAdvanceBlockInStream()}, but a specific position can be specified
   * and the stream pointer will be at that offset after this method completes.
   *
   * @param newPos the new position to set the stream to
   * @throws IOException if the stream at the specified position cannot be opened
   */
  private void seekBlockInStream(long newPos) throws IOException {
    long oldBlockId = getCurrentBlockId();
    mPos = newPos;
    closeCacheStream();
    long currentBlockId = getCurrentBlockId();

    if (oldBlockId != currentBlockId) {
      updateBlockInStream(currentBlockId);
      // Reading next block entirely.
      if (mPos % mBlockSize == 0 && mShouldCacheCurrentBlock) {
        try {
          long blockSize = getCurrentBlockSize();
          WorkerNetAddress address = mLocationPolicy.getWorkerForNextBlock(
              mContext.getAlluxioBlockStore().getWorkerInfoList(), blockSize);
          mCurrentCacheStream =
              mContext.getAlluxioBlockStore().getOutStream(currentBlockId, blockSize, address);
        } catch (IOException e) {
          logCacheStreamIOException(e);
          mShouldCacheCurrentBlock = false;
        } catch (AlluxioException e) {
          LOG.warn(BLOCK_ID_NOT_CACHED, currentBlockId, e);
          mShouldCacheCurrentBlock = false;
        }
      } else {
        mShouldCacheCurrentBlock = false;
      }
    }
  }

  /**
   * Helper method to {@link #checkAndAdvanceBlockInStream()} and {@link #seekBlockInStream(long)}.
   * The current {@link BlockInStream} will be closed and a new {@link BlockInStream} for the given
   * blockId will be opened at position 0.
   *
   * @param blockId blockId to set the {@link #mCurrentBlockInStream} to read
   * @throws IOException if the next {@link BlockInStream} cannot be obtained
   */
  private void updateBlockInStream(long blockId) throws IOException {
    if (mCurrentBlockInStream != null) {
      mCurrentBlockInStream.close();
    }
    try {
      if (mAlluxioStorageType.isPromote()) {
        try {
          mContext.getAlluxioBlockStore().promote(blockId);
        } catch (IOException e) {
          // Failed to promote
          LOG.warn("Promotion of block with ID {} failed.", blockId, e);
        }
      }
      mCurrentBlockInStream = mContext.getAlluxioBlockStore().getInStream(blockId);
      mShouldCacheCurrentBlock =
          !(mCurrentBlockInStream instanceof LocalBlockInStream) && mAlluxioStorageType.isStore();
    } catch (IOException e) {
      LOG.debug("Failed to get BlockInStream for block with ID {}, using UFS instead. {}",
          blockId, e);
      if (!mStatus.isPersisted()) {
        LOG.error("Could not obtain data for block with ID {} from Alluxio."
            + " The block is also not available in the under storage.", blockId);
        throw e;
      }
      long blockStart = BlockId.getSequenceNumber(blockId) * mBlockSize;
      mCurrentBlockInStream =
          new UnderStoreBlockInStream(blockStart, mBlockSize, mStatus.getUfsPath());
      mShouldCacheCurrentBlock = mAlluxioStorageType.isStore();
    }
  }
}
