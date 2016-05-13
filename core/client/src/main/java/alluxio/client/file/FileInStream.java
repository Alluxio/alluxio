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
  protected final AlluxioStorageType mAlluxioStorageType;
  /** Standard block size in bytes of the file, guaranteed for all but the last block. */
  protected final long mBlockSize;
  /** The location policy for CACHE type of read into Alluxio. */
  protected final FileWriteLocationPolicy mLocationPolicy;
  /** Total length of the file in bytes. */
  protected final long mFileLength;
  /** File System context containing the {@link FileSystemMasterClient} pool. */
  protected final FileSystemContext mContext;
  /** File information. */
  protected URIStatus mStatus;
  /** Constant error message for block ID not cached. */
  protected static final String BLOCK_ID_NOT_CACHED =
      "The block with ID {} could not be cached into Alluxio storage.";
  /** Error message for cache collision. */
  private static final String BLOCK_ID_EXISTS_SO_NOT_CACHED =
      "The block with ID {} is already stored in the target worker, canceling the cache request.";

  /** If the stream is closed, this can only go from false to true. */
  protected boolean mClosed;
  /**
   * Current position of the file instream.
   */
  protected long mPos;

  /** Include partially read blocks if Alluxio is configured to store blocks in Alluxio storage. */
  private final boolean mShouldCachePartiallyReadBlock;
  /** Whether to cache blocks in this file into Alluxio. */
  private final boolean mShouldCache;

  // The following 3 fields must be kept in sync. They are only updated in updateStreams together.
  /** Current {@link BlockInStream} backing this stream. */
  protected BlockInStream mCurrentBlockInStream;
  /** Current {@link BufferedBlockOutStream} writing the data into Alluxio. */
  protected BufferedBlockOutStream mCurrentCacheStream;
  /** The blockId used in the block streams. */
  private long mStreamBlockId;

  /**
   * Creates a new file input stream.
   *
   * @param status the file status
   * @param options the client options
   * @return the created {@link FileInStream} instance
   */
  public static FileInStream create(URIStatus status, InStreamOptions options) {
    if (status.getLength() == Constants.UNKNOWN_SIZE) {
      return new UnknownLengthFileInStream(status, options);
    }
    return new FileInStream(status, options);
  }

 /**
   * Creates a new file input stream.
   *
   * @param status the file status
   * @param options the client options
   */
  protected FileInStream(URIStatus status, InStreamOptions options) {
    mStatus = status;
    mBlockSize = status.getBlockSizeBytes();
    mFileLength = status.getLength();
    mContext = FileSystemContext.INSTANCE;
    mAlluxioStorageType = options.getAlluxioStorageType();
    mShouldCache = mAlluxioStorageType.isStore();
    mShouldCachePartiallyReadBlock = options.isCachePartiallyReadBlock();
    mClosed = false;
    mLocationPolicy = options.getLocationPolicy();
    if (mShouldCache) {
      Preconditions.checkNotNull(options.getLocationPolicy(),
          PreconditionMessage.FILE_WRITE_LOCATION_POLICY_UNSPECIFIED);
    }
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    updateStreams();
    if (mCurrentCacheStream != null && mShouldCachePartiallyReadBlock) {
      readCurrentBlockToEnd();
    }
    if (mCurrentBlockInStream != null) {
      mCurrentBlockInStream.close();
    }
    closeOrCancelCacheStream();
    mClosed = true;
  }

  @Override
  public int read() throws IOException {
    if (remaining() <= 0) {
      return -1;
    }
    updateStreams();
    Preconditions.checkState(mCurrentBlockInStream != null, "Reached EOF unexpectedly.");

    int data = mCurrentBlockInStream.read();

    if (data == -1) {
      // The underlying stream is done.
      return -1;
    }

    mPos++;
    if (mCurrentCacheStream != null) {
      try {
        mCurrentCacheStream.write(data);
      } catch (IOException e) {
        handleCacheStreamIOException(e);
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
    } else if (remaining() <= 0) {
      return -1;
    }

    int currentOffset = off;
    int bytesLeftToRead = len;

    while (bytesLeftToRead > 0 && remaining() > 0) {
      updateStreams();
      if (mCurrentBlockInStream == null) {
        // EOF is reached.
        break;
      }
      int bytesToRead = (int) Math.min(bytesLeftToRead, mCurrentBlockInStream.remaining());
      Preconditions.checkState(bytesToRead > 0);

      int bytesRead = mCurrentBlockInStream.read(b, currentOffset, bytesToRead);
      if (bytesRead > 0) {
        if (mCurrentCacheStream != null) {
          try {
            mCurrentCacheStream.write(b, currentOffset, bytesRead);
          } catch (IOException e) {
            handleCacheStreamIOException(e);
          }
        }
        mPos += bytesRead;
        bytesLeftToRead -= bytesRead;
        currentOffset += bytesRead;
      }
    }

    if (bytesLeftToRead == len && mCurrentBlockInStream.remaining() == 0) {
      // Nothing was read, and the underlying stream is done.
      return -1;
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
    Preconditions
        .checkArgument(pos <= maxSeekPosition(), PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE,
            pos);
    if (!mShouldCachePartiallyReadBlock) {
      seekInternal(pos);
    } else {
      seekInternalWithCachingPartiallyReadBlock(pos);
    }
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(n, remaining());
    seekInternal(mPos + toSkip);
    return toSkip;
  }

  /**
   * @return the maximum position to seek to
   */
  protected long maxSeekPosition() {
    return mFileLength;
  }

  /**
   * @param pos the position to check
   * @return the block size in bytes for the given pos, used for worker allocation
   */
  protected long getBlockSizeAllocation(long pos) {
    return getBlockSize(pos);
  }

  /**
   * Creates and returns a {@link BlockInStream} for the UFS.
   *
   * @param blockStart the offset to start the block from
   * @param length the length of the block
   * @param path the UFS path
   * @return the {@link BlockInStream} for the UFS
   * @throws IOException if the stream cannot be created
   */
  protected BlockInStream createUnderStoreBlockInStream(long blockStart, long length, String path)
      throws IOException {
    return new UnderStoreBlockInStream(blockStart, length, mBlockSize, path);
  }

  /**
   * If we are not in the last block or if the last block is equal to the normal block size,
   * return the normal block size. Otherwise return the block size of the last block.
   *
   * @param pos the position to get the block size for
   * @return the size of the block that covers pos
   */
  protected long getBlockSize(long pos) {
    // The size of the last block, 0 if it is equal to the normal block size
    long lastBlockSize = mFileLength % mBlockSize;
    if (mFileLength - pos > lastBlockSize) {
      return mBlockSize;
    } else {
      return lastBlockSize;
    }
  }

  /**
   * Checks whether block instream and cache outstream should be updated.
   * This function is only called by {@link #updateStreams()}.
   *
   * @param currentBlockId cached result of {@link #getCurrentBlockId()}
   * @return true if the block stream should be updated
   */
  protected boolean shouldUpdateStreams(long currentBlockId) {
    if (mCurrentBlockInStream == null || currentBlockId != mStreamBlockId) {
      return true;
    }
    if (mCurrentCacheStream != null) {
      Preconditions.checkState(mCurrentBlockInStream.remaining() == mCurrentCacheStream.remaining(),
          "BlockInStream and CacheStream is out of sync %d %d", mCurrentBlockInStream.remaining(),
          mCurrentCacheStream.remaining());
    }
    return mCurrentBlockInStream.remaining() == 0;
  }

  /**
   * Closes or cancels {@link #mCurrentCacheStream}.
   *
   * @throws IOException if the close or cancel fails
   */
  private void closeOrCancelCacheStream() throws IOException {
    if (mCurrentCacheStream == null) {
      return;
    }
    if (mCurrentCacheStream.remaining() == 0) {
      mCurrentCacheStream.close();
    } else {
      mCurrentCacheStream.cancel();
    }
    mCurrentCacheStream = null;
  }

  /**
   * @return the current block id based on mPos, -1 if at the end of the file
   */
  private long getCurrentBlockId() {
    if (remaining() <= 0) {
      return -1;
    }
    int index = (int) (mPos / mBlockSize);
    Preconditions
        .checkState(index < mStatus.getBlockIds().size(), PreconditionMessage.ERR_BLOCK_INDEX);
    return mStatus.getBlockIds().get(index);
  }

  /**
   * Logs IO exceptions thrown in response to the worker cache request. If the exception is not an
   * expected exception, a warning will be logged with the stack trace.
   */
  private void handleCacheStreamIOException(IOException e) throws IOException {
    if (e.getCause() instanceof BlockAlreadyExistsException) {
      LOG.warn(BLOCK_ID_EXISTS_SO_NOT_CACHED, getCurrentBlockId());
    } else {
      LOG.warn(BLOCK_ID_NOT_CACHED, getCurrentBlockId(), e);
    }
    closeOrCancelCacheStream();
  }

  /**
   * Only updates {@link #mCurrentCacheStream}, {@link #mCurrentBlockInStream} and
   * {@link #mStreamBlockId} to be in sync with the current block (i.e.
   * {@link #getCurrentBlockId()}).
   * If this method is called multiple times, the subsequent invokes become no-op.
   * Call this function every read and seek unless you are sure about the block streams are
   * up-to-date.
   *
   * @throws IOException if the next cache stream or block stream cannot be created
   */
  private void updateStreams() throws IOException {
    long currentBlockId = getCurrentBlockId();
    if (shouldUpdateStreams(currentBlockId)) {
      // The following two function handle negative currentBlockId (i.e. the end of file)
      // correctly.
      updateBlockInStream(currentBlockId);
      updateCacheStream(currentBlockId);
      mStreamBlockId = currentBlockId;
    }
  }

  /**
   * Updates {@link #mCurrentCacheStream}. The following preconditions are checked inside:
   *   1. {@link #mCurrentCacheStream} is either done or null.
   *   2. EOF is reached or {@link #mCurrentBlockInStream} must be valid.
   * After this call, {@link #mCurrentCacheStream} is either null or freshly created.
   * {@link #mCurrentCacheStream} is created only if the block is not cached in a chosen machine
   * and mPos is at the beginning of a block.
   * This function is only called by {@link #updateStreams()}.
   *
   * @param blockId cached result of {@link #getCurrentBlockId()}
   * @throws IOException if the next cache stream cannot be created
   */
  private void updateCacheStream(long blockId) throws IOException {
    // We should really only close a cache stream here. This check is to verify this.
    Preconditions.checkState(mCurrentCacheStream == null || mCurrentCacheStream.remaining() == 0);
    closeOrCancelCacheStream();
    Preconditions.checkState(mCurrentCacheStream == null);

    if (blockId < 0) {
      // End of file.
      return;
    }
    Preconditions.checkNotNull(mCurrentBlockInStream);
    if (!mShouldCache || mCurrentBlockInStream instanceof LocalBlockInStream) {
      return;
    }

    // Unlike updateBlockInStream below, we never start a block cache stream if mPos is in the
    // middle of a block.
    if (mPos % mBlockSize != 0) {
      return;
    }

    try {
      WorkerNetAddress address = mLocationPolicy.getWorkerForNextBlock(
          mContext.getAlluxioBlockStore().getWorkerInfoList(), getBlockSizeAllocation(mPos));
      // Don't cache the block to somewhere that already has it.
      // TODO(andrew,peis): Filter the workers provided to the location policy to not include
      // workers which already contain the block. See ALLUXIO-1816.
      if (mCurrentBlockInStream instanceof RemoteBlockInStream) {
        WorkerNetAddress readAddress =
            ((RemoteBlockInStream) mCurrentBlockInStream).getWorkerNetAddress();
        // Try to avoid an RPC.
        if (readAddress.equals(address)) {
          return;
        }
        BlockInfo blockInfo = mContext.getAlluxioBlockStore().getInfo(blockId);
        for (BlockLocation location : blockInfo.getLocations()) {
          if (address.equals(location.getWorkerAddress())) {
            return;
          }
        }
      }
      // If we reach here, we need to cache.
      mCurrentCacheStream =
          mContext.getAlluxioBlockStore().getOutStream(blockId, getBlockSize(mPos), address);
    } catch (IOException e) {
      handleCacheStreamIOException(e);
    } catch (AlluxioException e) {
      LOG.warn(BLOCK_ID_NOT_CACHED, blockId, e);
    }
  }

  /**
   * Update {@link #mCurrentBlockInStream} to be in-sync with mPos's block. The new block
   * stream created with be at position 0.
   * This function is only called in {@link #updateStreams()}.
   *
   * @param blockId cached result of {@link #getCurrentBlockId()}
   * @throws IOException if the next {@link BlockInStream} cannot be obtained
   */
  private void updateBlockInStream(long blockId) throws IOException {
    if (mCurrentBlockInStream != null) {
      mCurrentBlockInStream.close();
      mCurrentBlockInStream = null;
    }

    // blockId = -1 if mPos = EOF.
    if (blockId < 0) {
      return;
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
    } catch (IOException e) {
      LOG.debug("Failed to get BlockInStream for block with ID {}, using UFS instead. {}", blockId,
          e);
      if (!mStatus.isPersisted()) {
        LOG.error("Could not obtain data for block with ID {} from Alluxio."
            + " The block is also not available in the under storage.", blockId);
        throw e;
      }
      long blockStart = BlockId.getSequenceNumber(blockId) * mBlockSize;
      mCurrentBlockInStream =
          createUnderStoreBlockInStream(blockStart, getBlockSize(blockStart), mStatus.getUfsPath());
    }
  }

  /**
   * Seeks to a file position. Blocks are not cached unless they are fully read. This is only called
   * by {@link FileInStream#seek}.
   *
   * @param pos The position to seek to. It is guaranteed to be valid (pos >= 0 && pos != mPos &&
   *            pos <= mFileLength)
   * @throws IOException if the seek fails due to an error accessing the stream at the position
   */
  private void seekInternal(long pos) throws IOException {
    closeOrCancelCacheStream();
    mPos = pos;
    updateStreams();
    if (mCurrentBlockInStream != null) {
      mCurrentBlockInStream.seek(mPos % mBlockSize);
    } else {
      Preconditions.checkState(remaining() == 0);
    }
  }

  /**
   * Seeks to a file position. Blocks are cached even if they are not fully read. This is only
   * called by {@link FileInStream#seek}.
   * Invariant: if the current block is to be cached, [0, mPos) should have been cached already.
   *
   * @param pos The position to seek to. It is guaranteed to be valid (pos >= 0 && pos != mPos &&
   *            pos <= mFileLength).
   * @throws IOException if the seek fails due to an error accessing the stream at the position
   */
  private void seekInternalWithCachingPartiallyReadBlock(long pos) throws IOException {
    // Precompute this because mPos will be updated several times in this function.
    boolean isInCurrentBlock = pos / mBlockSize == mPos / mBlockSize;

    // Make sure that mCurrentBlockInStream and mCurrentCacheStream is updated.
    // mPos is not updated here.
    updateStreams();

    if (mCurrentCacheStream != null) {
      // Cache till pos if seeking forward within the current block. Otheriwse cache the whole
      // block.
      readCurrentBlockToPos(pos > mPos ? pos : Long.MAX_VALUE);

      // Early return if we are at pos already. This happens if we seek forward with caching
      // enabled for this block.
      if (mPos == pos) {
        return;
      }
      // The early return above guarantees that we won't close an incomplete cache stream.
      Preconditions.checkState(mCurrentCacheStream == null || mCurrentCacheStream.remaining() == 0);
      closeOrCancelCacheStream();
    }

    // If seeks within the current block, directly seeks to pos if we are not yet there.
    // If seeks outside the current block, seek to the beginning of that block first, then
    // cache the prefix (pos % mBlockSize) of that block.
    if (isInCurrentBlock) {
      mPos = pos;
      // updateStreams is necessary when pos = mFileLength.
      updateStreams();
      if (mCurrentBlockInStream != null) {
        mCurrentBlockInStream.seek(mPos % mBlockSize);
      } else {
        Preconditions.checkState(remaining() == 0);
      }
    } else {
      mPos = pos / mBlockSize * mBlockSize;
      updateStreams();
      if (mCurrentCacheStream != null) {
        readCurrentBlockToPos(pos);
      } else if (mCurrentBlockInStream != null) {
        mPos = pos;
        mCurrentBlockInStream.seek(mPos % mBlockSize);
      } else {
        Preconditions.checkState(remaining() == 0);
      }
    }
  }

  /**
   * Reads till the file offset (mPos) equals pos or the end of the current block (whichever is
   * met first) if pos > mPos. Otherwise no-op.
   *
   * @param pos file offset
   * @throws IOException if read or cache write fails
   */
  private void readCurrentBlockToPos(long pos) throws IOException {
    Preconditions.checkNotNull(mCurrentBlockInStream);
    Preconditions.checkNotNull(mCurrentCacheStream);
    long len = Math.min(pos - mPos, mCurrentBlockInStream.remaining());
    if (len <= 0) {
      return;
    }

    byte[] buffer = new byte[Math.min(Constants.MB, (int) len)];
    do {
      len -= read(buffer);
    } while (len > 0);
  }

  /**
   * Reads the remaining of the current block.
   * @throws IOException if read or cache write fails
   */
  private void readCurrentBlockToEnd() throws IOException {
    readCurrentBlockToPos(Long.MAX_VALUE);
  }
}
