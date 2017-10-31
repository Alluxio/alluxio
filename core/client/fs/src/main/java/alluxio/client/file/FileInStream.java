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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.Seekable;
import alluxio.annotation.PublicApi;
import alluxio.client.AlluxioStorageType;
import alluxio.client.BoundedStream;
import alluxio.client.PositionedReadable;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.block.stream.BlockInStream.BlockInStreamSource;
import alluxio.client.block.stream.BlockOutStream;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.status.NotFoundException;
import alluxio.master.block.BlockId;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
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
 * This class wraps the block in stream for each of the blocks in the file and abstracts the
 * switching between streams. The backing streams can read from Alluxio space in the local machine,
 * remote machines, or the under storage system.
 */
@PublicApi
@NotThreadSafe
public class FileInStream extends InputStream
    implements BoundedStream, Seekable, PositionedReadable {
  private static final Logger LOG = LoggerFactory.getLogger(FileInStream.class);

  private static final boolean PASSIVE_CACHE_ENABLED =
      Configuration.getBoolean(PropertyKey.USER_FILE_PASSIVE_CACHE_ENABLED);

  /** The instream options. */
  private final InStreamOptions mInStreamOptions;
  /** The outstream options. */
  private final OutStreamOptions mOutStreamOptions;
  /** How the data should be written into Alluxio space, if at all. */
  protected final AlluxioStorageType mAlluxioStorageType;
  /** Standard block size in bytes of the file, guaranteed for all but the last block. */
  protected final long mBlockSize;
  /** Total length of the file in bytes. */
  protected final long mFileLength;
  /** File system context containing the {@link FileSystemMasterClient} pool. */
  protected final FileSystemContext mContext;
  private final AlluxioBlockStore mBlockStore;
  /** File information. */
  protected final URIStatus mStatus;

  /** If the stream is closed, this can only go from false to true. */
  protected boolean mClosed;
  /** Current position of the file instream. */
  protected long mPos;

  /**
   * Caches the entire block even if only a portion of the block is read. Only valid when
   * mShouldCache is true.
   */
  private final boolean mCachePartiallyReadBlock;
  /** Whether to cache blocks in this file into Alluxio. */
  private final boolean mShouldCache;

  // The following 3 fields must be kept in sync. They are only updated in updateStreams together.
  /** Current block in stream backing this stream. */
  protected BlockInStream mCurrentBlockInStream;
  /**
   * Current block out stream writing the data into the local worker. This is only used when the in
   * stream reads from a remote worker.
   */
  protected BlockOutStream mCurrentCacheStream;
  /** The blockId used in the block streams. */
  private long mStreamBlockId;

  /** The read buffer in file seek. This is used in {@link #readCurrentBlockToEnd()}. */
  private final byte[] mSeekBuffer;

  /**
   * Creates a new file input stream.
   *
   * @param status the file status
   * @param options the client options
   * @param context file system context
   * @return the created {@link FileInStream} instance
   */
  public static FileInStream create(URIStatus status, InStreamOptions options,
      FileSystemContext context) {
    if (status.getLength() == Constants.UNKNOWN_SIZE) {
      return new UnknownLengthFileInStream(status, options, context);
    }
    return new FileInStream(status, options, context);
  }

  /**
   * Creates a new file input stream.
   *
   * @param status the file status
   * @param options the client options
   */
  protected FileInStream(URIStatus status, InStreamOptions options, FileSystemContext context) {
    mStatus = status;
    mInStreamOptions = options;
    mOutStreamOptions = OutStreamOptions.defaults();
    mBlockSize = status.getBlockSizeBytes();
    mFileLength = status.getLength();
    mContext = context;
    mAlluxioStorageType = options.getAlluxioStorageType();
    mShouldCache = mAlluxioStorageType.isStore();
    mCachePartiallyReadBlock = options.isCachePartiallyReadBlock();
    mClosed = false;
    if (mShouldCache) {
      Preconditions.checkNotNull(options.getCacheLocationPolicy(),
          PreconditionMessage.FILE_WRITE_LOCATION_POLICY_UNSPECIFIED);
    }

    int seekBufferSizeBytes = Math.max((int) options.getSeekBufferSizeBytes(), 1);
    mSeekBuffer = new byte[seekBufferSizeBytes];
    mBlockStore = AlluxioBlockStore.create(context);
    LOG.debug("Init FileInStream with options {}", options);
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    updateStreams();
    if (shouldCachePartiallyReadBlock()) {
      // cache only when the read is not from local worker and there is a local worker to cache
      if (canCacheToLocalWorker()) {
        readCurrentBlockToEnd();
      }
    }
    if (mCurrentBlockInStream != null) {
      mCurrentBlockInStream.close();
    }
    closeOrCancelCacheStream();
    mClosed = true;
  }

  @Override
  public long getPos() {
    return mPos;
  }

  @Override
  public int read() throws IOException {
    return readInternal();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return readInternal(b, off, len);
  }

  private int readInternal() throws IOException {
    if (remainingInternal() <= 0) {
      return -1;
    }
    updateStreams();
    Preconditions.checkState(mCurrentBlockInStream != null, PreconditionMessage.ERR_UNEXPECTED_EOF);

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
        handleCacheStreamException(e);
      }
    }
    return data;
  }

  private int readInternal(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    if (len == 0) {
      return 0;
    } else if (remainingInternal() <= 0) {
      return -1;
    }

    int currentOffset = off;
    int bytesLeftToRead = len;

    while (bytesLeftToRead > 0 && remainingInternal() > 0) {
      updateStreams();
      Preconditions.checkNotNull(mCurrentBlockInStream, PreconditionMessage.ERR_UNEXPECTED_EOF);
      int bytesToRead = (int) Math.min(bytesLeftToRead, mCurrentBlockInStream.remaining());

      int bytesRead;
      try {
        bytesRead = mCurrentBlockInStream.read(b, currentOffset, bytesToRead);
      } catch (IOException e) {
        throw AlluxioStatusException.fromIOException(e);
      }
      if (bytesRead > 0) {
        if (mCurrentCacheStream != null) {
          try {
            mCurrentCacheStream.write(b, currentOffset, bytesRead);
          } catch (IOException e) {
            handleCacheStreamException(e);
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
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    return positionedReadInternal(pos, b, off, len);
  }

  private int positionedReadInternal(long pos, byte[] b, int off, int len) throws IOException {
    if (pos < 0 || pos >= mFileLength) {
      return -1;
    }

    // If partial read cache is enabled, we fall back to the normal read.
    if (shouldCachePartiallyReadBlock()) {
      synchronized (this) {
        long oldPos = mPos;
        try {
          seek(pos);
          return read(b, off, len);
        } finally {
          seek(oldPos);
        }
      }
    }

    int lenCopy = len;

    while (len > 0) {
      if (pos >= mFileLength) {
        break;
      }
      long blockId = getBlockId(pos);
      long blockPos = pos % mBlockSize;
      try (BlockInStream bin = getBlockInStream(blockId)) {
        int bytesRead =
            bin.positionedRead(blockPos, b, off, (int) Math.min(mBlockSize - blockPos, len));
        Preconditions.checkState(bytesRead > 0, "No data is read before EOF");
        pos += bytesRead;
        off += bytesRead;
        len -= bytesRead;
      }
    }
    return lenCopy - len;
  }

  @Override
  public long remaining() {
    // WARNING: do not call remaining() directly within this file, use remainingInternal() instead
    // so that remaining() can be overridden by subclasses.
    return remainingInternal();
  }

  @Override
  public void seek(long pos) throws IOException {
    if (mPos == pos) {
      return;
    }
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions.checkArgument(pos <= maxSeekPosition(),
        PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(), pos);

    if (shouldCachePartiallyReadBlock()) {
      seekInternalWithCachingPartiallyReadBlock(pos);
    } else {
      seekInternal(pos);
    }
  }

  /**
   * @return if the partially-read block should be cached to the local worker
   */
  private boolean shouldCachePartiallyReadBlock() {
    return mShouldCache && mCachePartiallyReadBlock;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(n, remainingInternal());
    seek(mPos + toSkip);
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
   * If we are not in the last block or if the last block is equal to the normal block size, return
   * the normal block size. Otherwise return the block size of the last block.
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
   * Checks whether block instream and cache outstream should be updated. This function is only
   * called by {@link #updateStreams()}.
   *
   * @param currentBlockId cached result of {@link #getCurrentBlockId()}
   * @return true if the block stream should be updated
   */
  protected boolean shouldUpdateStreams(long currentBlockId) {
    if (mCurrentBlockInStream == null || currentBlockId != mStreamBlockId) {
      return true;
    }
    if (mCurrentCacheStream != null
        && mCurrentBlockInStream.remaining() != mCurrentCacheStream.remaining()) {
      throw new IllegalStateException(
          String.format("BlockInStream and CacheStream is out of sync %d %d.",
              mCurrentBlockInStream.remaining(), mCurrentCacheStream.remaining()));
    }
    return mCurrentBlockInStream.remaining() == 0;
  }

  /**
   * Closes or cancels {@link #mCurrentCacheStream}.
   */
  private void closeOrCancelCacheStream() {
    if (mCurrentCacheStream == null) {
      return;
    }
    try {
      if (mCurrentCacheStream.remaining() == 0) {
        mCurrentCacheStream.close();
      } else {
        mCurrentCacheStream.cancel();
      }
    } catch (NotFoundException e) {
      // This happens if two concurrent readers read trying to cache the same block. One cancelled
      // before the other. Then the other reader will see this exception since we only keep
      // one block per blockId in block worker.
      LOG.info("Block {} does not exist when being cancelled.", getCurrentBlockId());
    } catch (AlreadyExistsException e) {
      // This happens if two concurrent readers trying to cache the same block. One successfully
      // committed. The other reader sees this.
      LOG.info("Block {} exists.", getCurrentBlockId());
    } catch (IOException e) {
      // This happens when there are any other cache stream close/cancel related errors (e.g.
      // server unreachable due to network partition, server busy due to Alluxio worker is
      // busy, timeout due to congested network etc). But we want to proceed since we want
      // the user to continue reading when one Alluxio worker is having trouble.
      LOG.info("Closing or cancelling the cache stream encountered IOException {}, reading from "
          + "the regular stream won't be affected.", e.getMessage());
    }
    mCurrentCacheStream = null;
  }

  /**
   * @return the current block id based on mPos, -1 if at the end of the file
   */
  private long getCurrentBlockId() {
    if (remainingInternal() <= 0) {
      return -1;
    }
    return getBlockId(mPos);
  }

  /**
   * @param pos the pos
   * @return the block ID based on the pos
   */
  private long getBlockId(long pos) {
    int index = (int) (pos / mBlockSize);
    Preconditions.checkState(index < mStatus.getBlockIds().size(),
        PreconditionMessage.ERR_BLOCK_INDEX.toString(), index, pos, mStatus.getBlockIds().size());
    return mStatus.getBlockIds().get(index);
  }

  /**
   * Handles IO exceptions thrown in response to the worker cache request. Cache stream is closed or
   * cancelled after logging some messages about the exceptions.
   *
   * @param e the exception to handle
   */
  private void handleCacheStreamException(IOException e) {
    if (Throwables.getRootCause(e) instanceof AlreadyExistsException) {
      // This can happen if there are two readers trying to cache the same block. The first one
      // created the block (either as temp block or committed block). The second sees this
      // exception.
      LOG.info("The block with ID {} is already stored in the target worker, canceling the cache "
          + "request.", getCurrentBlockId());
    } else {
      LOG.warn("The block with ID {} could not be cached into Alluxio storage: {}",
          getCurrentBlockId(), e.toString());
    }
    closeOrCancelCacheStream();
  }

  /**
   * Only updates {@link #mCurrentCacheStream}, {@link #mCurrentBlockInStream} and
   * {@link #mStreamBlockId} to be in sync with the current block (i.e. {@link #getCurrentBlockId()}
   * ). If this method is called multiple times, the subsequent invokes become no-op. Call this
   * function every read and seek unless you are sure about the block streams are up-to-date.
   */
  private void updateStreams() throws IOException {
    long currentBlockId = getCurrentBlockId();
    if (shouldUpdateStreams(currentBlockId)) {
      // The following two function handle negative currentBlockId (i.e. the end of file)
      // correctly.
      updateBlockInStream(currentBlockId);
      if (PASSIVE_CACHE_ENABLED) {
        updateCacheStream(currentBlockId);
      }
      mStreamBlockId = currentBlockId;
    }
  }

  /**
   * Updates {@link #mCurrentCacheStream}. When {@code mShouldCache} is true, {@code FileInStream}
   * will create an {@code BlockOutStream} to cache the data read only if the block is read from a
   * remote worker and we have an available local worker. Note that when the block is in UFS but not
   * in Alluxio, the cache stream is not needed in the client because the worker caches directly the
   * block when it reads from UFS. And if the block is already in local worker, then the block
   * should not be cached again.
   *
   * The following preconditions are checked inside:
   * <ol>
   * <li>{@link #mCurrentCacheStream} is either done or null.</li>
   * <li>EOF is reached or {@link #mCurrentBlockInStream} must be valid.</li>
   * </ol>
   * After this call, {@link #mCurrentCacheStream} is either null or freshly created.
   * {@link #mCurrentCacheStream} is created only if the block is not cached in a chosen machine and
   * mPos is at the beginning of a block. This function is only called by {@link #updateStreams()}.
   *
   * @param blockId cached result of {@link #getCurrentBlockId()}
   */
  private void updateCacheStream(long blockId) {
    // We should really only close a cache stream here. This check is to verify this.
    Preconditions.checkState(mCurrentCacheStream == null || mCurrentCacheStream.remaining() == 0);
    closeOrCancelCacheStream();
    Preconditions.checkState(mCurrentCacheStream == null);

    if (blockId < 0) {
      // End of file.
      return;
    }
    Preconditions.checkNotNull(mCurrentBlockInStream, "mCurrentBlockInStream");

    // do not create cache stream when the block is not in remote worker
    if (!(mShouldCache && mCurrentBlockInStream.Source() == BlockInStreamSource.REMOTE)) {
      return;
    }

    // Unlike updateBlockInStream below, we never start a block cache stream if mPos is in the
    // middle of a block.
    if (mPos % mBlockSize != 0) {
      return;
    }

    try {
      // If this block is read from a remote worker, we should never cache except to a local worker.
      WorkerNetAddress localWorker = mContext.getLocalWorker();
      if (localWorker != null) {
        mCurrentCacheStream =
            mBlockStore.getOutStream(blockId, getBlockSize(mPos), localWorker, mOutStreamOptions);
      }
    } catch (IOException e) {
      handleCacheStreamException(e);
    }
  }

  /**
   * Update {@link #mCurrentBlockInStream} to be in-sync with mPos's block. This function is only
   * called in {@link #updateStreams()}.
   *
   * @param blockId cached result of {@link #getCurrentBlockId()}
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
    mCurrentBlockInStream = getBlockInStream(blockId);
  }

  /**
   * Gets the block in stream corresponding a block ID.
   *
   * @param blockId the block ID
   * @return the block in stream
   */
  private BlockInStream getBlockInStream(long blockId) throws IOException {
    Protocol.OpenUfsBlockOptions openUfsBlockOptions = null;
    boolean readFromUfs = mStatus.isPersisted();
    if (readFromUfs) {
      long blockStart = BlockId.getSequenceNumber(blockId) * mBlockSize;
      openUfsBlockOptions =
          Protocol.OpenUfsBlockOptions.newBuilder().setUfsPath(mStatus.getUfsPath())
              .setOffsetInFile(blockStart).setBlockSize(getBlockSize(blockStart))
              .setMaxUfsReadConcurrency(mInStreamOptions.getMaxUfsReadConcurrency())
              .setNoCache(!mInStreamOptions.getAlluxioStorageType().isStore())
              .setMountId(mStatus.getMountId()).build();
    }
    return mBlockStore.getInStream(blockId, openUfsBlockOptions, mInStreamOptions);
  }

  /**
   * Seeks to a file position. Blocks are not cached unless they are fully read. This is only called
   * by {@link FileInStream#seek}.
   *
   * @param pos The position to seek to. It is guaranteed to be valid (pos >= 0 && pos != mPos &&
   *        pos <= mFileLength)
   */
  private void seekInternal(long pos) throws IOException {
    closeOrCancelCacheStream();
    mPos = pos;
    updateStreams();
    if (mCurrentBlockInStream != null) {
      mCurrentBlockInStream.seek(mPos % mBlockSize);
    } else {
      Preconditions.checkState(remainingInternal() == 0);
    }
  }

  private long remainingInternal() {
    return mFileLength - mPos;
  }

  /**
   * Seeks to a file position with partial caching if needed. Blocks are cached even if they are not
   * fully read. This is only called by {@link FileInStream#seek}.
   *
   * The seek could involve at most two blocks: the current block and the target block. And it's
   * possible that the current block and the target block are the same block when the seek is within
   * the same block.
   *
   * The behavior for the current block:
   * <ol>
   * <li>If the current block is already available in the local worker, then the caching of the
   * current block is not needed. And if the seek position is within the current block, directly
   * move the position to the seek position. Otherwise the position is moved outside the current
   * block.</li>
   * <li>If the current block is not available locally, then the partial caching is needed. And data
   * needs to read from the current position till the seek position, when the position is within the
   * current block, otherwise reads the rest of the current block.</li>
   * <li>However, a caveat is that if this is the first seek before any data is read from the file
   * and this seeks is outside the first block, then the first block should not be cached.</li>
   * <li>Lastly, if the current block is from remote worker and there is no local worker then the
   * caching is not needed</li>
   * </ol>
   *
   * The behavior on the target block:
   * <ol>
   * <li>If the target block is already available in the local worker or it's from remote worker but
   * no local worker available, then the caching on the target block is not needed. Directly move
   * the position to the seek position.</li>
   * <li>Otherwise data from the beginning of the block till the seek position needs to be read and
   * cached.</li>
   * </ol>
   *
   * Invariant: if the current block is to be cached, [0, mPos) should have been cached already.
   *
   * @param pos The position to seek to. It is guaranteed to be valid (pos >= 0 && pos != mPos &&
   *        pos <= mFileLength).
   */
  private void seekInternalWithCachingPartiallyReadBlock(long pos) throws IOException {
    // Precompute this because mPos will be updated several times in this function.
    final boolean isInCurrentBlock = pos / mBlockSize == mPos / mBlockSize;

    if (isInCurrentBlock) {
      // the read is within the current block update stream to refresh the streams
      updateStreams();
      if (isReadFromLocalWorker()) {
        // no need to partial cache the current block, and the seek is within the block
        // so directly seeks to position.
        mPos = pos;
        // updateStreams is necessary when pos = mFileLength.
        updateStreams();
        if (mCurrentBlockInStream != null) {
          mCurrentBlockInStream.seek(mPos % mBlockSize);
        } else {
          Preconditions.checkState(remaining() == 0);
        }
        return;
      }
    }

    // cache the current block if neither of these conditions hold:
    // (1) this is the first seek before any read, and the seek is outside the first block
    // (2) the in stream reads from the local worker
    // (3) the in stream reads from a remote worker but there is no local worker
    boolean firstSeekOutsideFirstBlock =
        mPos == 0 && mCurrentBlockInStream == null && !isInCurrentBlock;
    if (!firstSeekOutsideFirstBlock && canCacheToLocalWorker()) {
      // Make sure that mCurrentBlockInStream and mCurrentCacheStream is updated.
      // mPos is not updated here.
      updateStreams();

      // Cache till pos if seeking forward within the current block. Otherwise cache the whole
      // block.
      if (isInCurrentBlock && pos > mPos) {
        readCurrentBlockToPos(pos);
      } else {
        readCurrentBlockToEnd();
      }

      // Early return if we are at pos already. This happens if we seek forward with caching
      // enabled for this block.
      if (mPos == pos) {
        return;
      }
      // The early return above guarantees that we won't close an incomplete cache stream.
      Preconditions.checkState(mCurrentCacheStream == null || mCurrentCacheStream.remaining() == 0);
      closeOrCancelCacheStream();
    }

    // lastly handle the target block
    // the seek is outside the current block, seek to the beginning of that block first
    mPos = pos / mBlockSize * mBlockSize;
    updateStreams();
    if (canCacheToLocalWorker()) {
      // cache till the seek position of the block unless
      // (1) the in stream reads from the local worker
      // (2) the in stream reads from a remote worker but there is no local worker
      readCurrentBlockToPos(pos);
    } else if (mCurrentBlockInStream != null) {
      // otherwise directly seek to the position
      seekInternal(pos);
    } else {
      Preconditions.checkState(remaining() == 0);
    }
  }

  /**
   * The client can cache to the local worker if the data is not already in local worker, neither
   * read from remote but no local worker available.
   */
  private boolean canCacheToLocalWorker() throws IOException {
    return !isReadFromLocalWorker() && !isRemoteReadButNoLocalWorker();
  }

  private boolean isReadFromLocalWorker() {
    return mCurrentBlockInStream != null
        && mCurrentBlockInStream.Source() == BlockInStreamSource.LOCAL;
  }

  private boolean isRemoteReadButNoLocalWorker() throws IOException {
    return mCurrentBlockInStream != null
        && mCurrentBlockInStream.Source() == BlockInStreamSource.REMOTE
        && mContext.getLocalWorker() == null;
  }

  /**
   * Reads till the file offset (mPos) equals pos or the end of the current block (whichever is met
   * first) if pos > mPos. Otherwise no-op. Writes to cache stream if the stream is not null.
   *
   * @param pos file offset
   */
  private void readCurrentBlockToPos(long pos) throws IOException {
    if (mCurrentBlockInStream == null) {
      return;
    }
    long len = Math.min(pos - mPos, mCurrentBlockInStream.remaining());
    if (len <= 0) {
      return;
    }

    do {
      // Account for the last read which might be less than mSeekBufferSizeBytes bytes.
      int bytesRead = readInternal(mSeekBuffer, 0, (int) Math.min(mSeekBuffer.length, len));
      Preconditions.checkState(bytesRead > 0, PreconditionMessage.ERR_UNEXPECTED_EOF);
      len -= bytesRead;
    } while (len > 0);
  }

  /**
   * Reads the remaining of the current block. Writes to cache stream if the stream is not null.
   */
  private void readCurrentBlockToEnd() throws IOException {
    readCurrentBlockToPos(Long.MAX_VALUE);
  }
}
