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

package alluxio.worker.block;

import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.IdUtils;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.UnderFileSystemBlockMeta;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class implements a {@link BlockReader} to read a block directly from UFS, and
 * optionally cache the block to the Alluxio worker if the whole block it is read.
 */
@NotThreadSafe
public final class UnderFileSystemBlockReader extends BlockReader {
  private static final Logger LOG = LoggerFactory.getLogger(UnderFileSystemBlockReader.class);

  /** Metrics. */
  private static final Counter BLOCKS_READ_UFS =
      MetricsSystem.counter(MetricKey.WORKER_BLOCKS_READ_UFS.getName());

  private final Counter mUfsBytesRead;
  private final Meter mUfsBytesReadThroughput;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();

  /** The initial size of the block allocated in Alluxio storage when the block is cached. */
  private final long mInitialBlockSize;
  /** The block metadata for the UFS block. */
  private final UnderFileSystemBlockMeta mBlockMeta;
  /** The Local block store. It is used to interact with Alluxio. */
  private final BlockStore mLocalBlockStore;
  /** The cache for all ufs instream. */
  private final UfsInputStreamCache mUfsInstreamCache;
  /** The ufs client resource. */
  private final CloseableResource<UnderFileSystem> mUfsResource;
  private final boolean mIsPositionShort;

  /** The input stream to read from UFS. */
  private InputStream mUnderFileSystemInputStream;
  /** The block writer to write the block to Alluxio. */
  private BlockWriter mBlockWriter;
  /** If set, the reader is closed and should not be used afterwards. */
  private boolean mClosed;

  /**
   * The position of mUnderFileSystemInputStream (if not null) is blockStart + mInStreamPos.
   * When mUnderFileSystemInputStream is not set, this is set to -1 (an invalid state) when
   * mUnderFileSystemInputStream is null. Check mUnderFileSystemInputStream directly to see whether
   * that is valid instead of relying on this invalid state of the position to be safe.
   */
  private long mInStreamPos;

  /**
   * Creates an instance of {@link UnderFileSystemBlockReader} and initializes it with a reading
   * offset.
   *
   * @param blockMeta the block meta
   * @param offset the position within the block to start the read
   * @param localBlockStore the Local block store
   * @param ufsClient the manager of ufs
   * @param positionShort whether the client op is a positioned read to a small buffer
   * @param ufsInStreamCache the UFS in stream cache
   * @param ufsBytesRead counter metric to track ufs bytes read
   * @param ufsBytesReadThroughput meter metric to track bytes read throughput
   * @return the block reader
   */
  public static UnderFileSystemBlockReader create(UnderFileSystemBlockMeta blockMeta, long offset,
      boolean positionShort, BlockStore localBlockStore, UfsManager.UfsClient ufsClient,
      UfsInputStreamCache ufsInStreamCache, Counter ufsBytesRead, Meter ufsBytesReadThroughput)
      throws IOException {
    UnderFileSystemBlockReader ufsBlockReader =
        new UnderFileSystemBlockReader(blockMeta, positionShort, localBlockStore, ufsClient,
            ufsInStreamCache, ufsBytesRead, ufsBytesReadThroughput);
    ufsBlockReader.init(offset);
    return ufsBlockReader;
  }

  /**
   * Creates an instance of {@link UnderFileSystemBlockReader}.
   *
   * @param blockMeta the block meta
   * @param localBlockStore the Local block store
   * @param ufsClient the ufs client
   * @param positionShort whether the client op is a positioned read to a small buffer
   * @param ufsInStreamCache the UFS in stream cache
   * @param ufsBytesRead counter metric to track ufs bytes read
   * @param ufsBytesReadThroughput meter metric to track bytes read throughput
   */
  private UnderFileSystemBlockReader(UnderFileSystemBlockMeta blockMeta, boolean positionShort,
      BlockStore localBlockStore, UfsManager.UfsClient ufsClient,
      UfsInputStreamCache ufsInStreamCache, Counter ufsBytesRead, Meter ufsBytesReadThroughput) {
    mInitialBlockSize = blockMeta.getBlockSize();
    mBlockMeta = blockMeta;
    mLocalBlockStore = localBlockStore;
    mInStreamPos = -1;
    mUfsInstreamCache = ufsInStreamCache;
    mUfsResource = ufsClient.acquireUfsResource();
    mIsPositionShort = positionShort;
    mUfsBytesRead = ufsBytesRead;
    mUfsBytesReadThroughput = ufsBytesReadThroughput;
  }

  /**
   * Initializes the reader. This is only called in the factory method.
   *
   * @param offset the position within the block to start the read
   */
  private void init(long offset) throws IOException {
    updateUnderFileSystemInputStream(offset);
    updateBlockWriter(offset);
  }

  @Override
  public ReadableByteChannel getChannel() {
    throw new UnsupportedOperationException("UFSFileBlockReader#getChannel is not supported");
  }

  @Override
  public long getLength() {
    return mBlockMeta.getBlockSize();
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    Preconditions.checkState(!mClosed);
    updateUnderFileSystemInputStream(offset);
    updateBlockWriter(offset);

    long bytesToRead = Math.min(length, mBlockMeta.getBlockSize() - offset);
    if (bytesToRead <= 0) {
      return ByteBuffer.allocate(0);
    }
    byte[] data = new byte[(int) bytesToRead];
    int bytesRead = 0;
    Preconditions.checkNotNull(mUnderFileSystemInputStream, "mUnderFileSystemInputStream");
    while (bytesRead < bytesToRead) {
      int read;
      try {
        read = mUnderFileSystemInputStream.read(data, bytesRead, (int) (bytesToRead - bytesRead));
      } catch (IOException e) {
        throw AlluxioStatusException.fromIOException(e);
      }
      if (read == -1) {
        break;
      }
      bytesRead += read;
    }
    mInStreamPos += bytesRead;

    // We should always read the number of bytes as expected since the UFS file length (hence block
    // size) should be always accurate.
    Preconditions
        .checkState(bytesRead == bytesToRead, PreconditionMessage.NOT_ENOUGH_BYTES_READ.toString(),
            bytesRead, bytesToRead, mBlockMeta.getUnderFileSystemPath());
    if (mBlockWriter != null && mBlockWriter.getPosition() < mInStreamPos) {
      try {
        Preconditions.checkState(mBlockWriter.getPosition() >= offset);
        mLocalBlockStore.requestSpace(mBlockMeta.getSessionId(), mBlockMeta.getBlockId(),
            mInStreamPos - mBlockWriter.getPosition());
        ByteBuffer buffer = ByteBuffer.wrap(data, (int) (mBlockWriter.getPosition() - offset),
            (int) (mInStreamPos - mBlockWriter.getPosition()));
        mBlockWriter.append(buffer.duplicate());
      } catch (Exception e) {
        LOG.warn("Failed to cache data read from UFS (on read()): {}", e.toString());
        try {
          cancelBlockWriter();
        } catch (IOException ee) {
          LOG.error("Failed to cancel block writer:", ee);
        }
      }
    }
    mUfsBytesRead.inc(bytesRead);
    mUfsBytesReadThroughput.mark(bytesRead);
    return ByteBuffer.wrap(data, 0, bytesRead);
  }

  /**
   * This interface is supposed to be used for sequence block reads.
   *
   * @param buf the byte buffer
   * @return the number of bytes read, -1 if it reaches EOF and none was read
   */
  @Override
  public int transferTo(ByteBuf buf) throws IOException {
    Preconditions.checkState(!mClosed);
    if (mUnderFileSystemInputStream == null) {
      return -1;
    }
    if (mBlockMeta.getBlockSize() <= mInStreamPos) {
      return -1;
    }
   // Make a copy of the state to keep track of what we have read in this transferTo call.
    ByteBuf bufCopy = null;
    if (mBlockWriter != null) {
      bufCopy = buf.duplicate();
      bufCopy.readerIndex(bufCopy.writerIndex());
    }
    int bytesToRead =
        (int) Math.min(buf.writableBytes(), mBlockMeta.getBlockSize() - mInStreamPos);
    int bytesRead = buf.writeBytes(mUnderFileSystemInputStream, bytesToRead);
    if (bytesRead <= 0) {
      return bytesRead;
    }

    mInStreamPos += bytesRead;

    if (mBlockWriter != null && bufCopy != null) {
      try {
        bufCopy.writerIndex(buf.writerIndex());
        while (bufCopy.readableBytes() > 0) {
          mLocalBlockStore.requestSpace(mBlockMeta.getSessionId(), mBlockMeta.getBlockId(),
              mInStreamPos - mBlockWriter.getPosition());
          mBlockWriter.append(bufCopy);
        }
      } catch (Exception e) {
        LOG.warn("Failed to cache data read from UFS (on transferTo()): {}", e.toString());
        cancelBlockWriter();
      }
    }
    mUfsBytesRead.inc(bytesRead);
    mUfsBytesReadThroughput.mark(bytesRead);
    return bytesRead;
  }

  /**
   * Closes the block reader. After this, this block reader should not be used anymore.
   * This is recommended to be called after the client finishes reading the block. It is usually
   * triggered when the client unlocks the block.
   */
  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }

    super.close();
    try {
      // This aborts the block if the block is not fully read.
      updateBlockWriter(mBlockMeta.getBlockSize());

      if (mUnderFileSystemInputStream != null) {
        mUfsInstreamCache.release(mUnderFileSystemInputStream);
        mUnderFileSystemInputStream = null;
      }

      if (mBlockWriter != null) {
        mBlockWriter.close();
      }

      mUfsResource.close();
    } finally {
      mClosed = true;
      BLOCKS_READ_UFS.inc();
    }
  }

  @Override
  public boolean isClosed() {
    return mClosed;
  }

  @Override
  public String getLocation() {
    return mBlockMeta.getUnderFileSystemPath();
  }

  /**
   * Updates the UFS input stream given an offset to read.
   *
   * @param offset the read offset within the block
   */
  private void updateUnderFileSystemInputStream(long offset) throws IOException {
    if ((mUnderFileSystemInputStream != null) && offset != mInStreamPos) {
      mUfsInstreamCache.release(mUnderFileSystemInputStream);
      mUnderFileSystemInputStream = null;
      mInStreamPos = -1;
    }

    if (mUnderFileSystemInputStream == null && offset < mBlockMeta.getBlockSize()) {
      UnderFileSystem ufs = mUfsResource.get();
      mUnderFileSystemInputStream = mUfsInstreamCache
          .acquire(ufs, mBlockMeta.getUnderFileSystemPath(),
              IdUtils.fileIdFromBlockId(mBlockMeta.getBlockId()),
              OpenOptions.defaults().setOffset(mBlockMeta.getOffset() + offset)
                  .setPositionShort(mIsPositionShort));
      mInStreamPos = offset;
    }
  }

  /**
   * Closes the current block writer, cleans up its temp block and sets it to null.
   */
  private void cancelBlockWriter() throws IOException {
    if (mBlockWriter == null) {
      return;
    }
    try {
      mBlockWriter.close();
      mBlockWriter = null;
      mLocalBlockStore.abortBlock(mBlockMeta.getSessionId(), mBlockMeta.getBlockId());
    } catch (BlockDoesNotExistException e) {
      // This can only happen when the session is expired.
      LOG.warn("Block {} does not exist when being aborted. The session may have expired.",
          mBlockMeta.getBlockId());
    } catch (BlockAlreadyExistsException | InvalidWorkerStateException | IOException e) {
      // We cannot skip the exception here because we need to make sure that the user of this
      // reader does not commit the block if it fails to abort the block.
      throw AlluxioStatusException.fromCheckedException(e);
    }
  }

  /**
   * Updates the block writer given an offset to read. If the offset is beyond the current
   * position of the block writer, the block writer will be aborted.
   *
   * @param offset the read offset
   */
  private void updateBlockWriter(long offset) throws IOException {
    if (mBlockWriter != null && offset > mBlockWriter.getPosition()) {
      cancelBlockWriter();
    }
    try {
      if (mBlockWriter == null && offset == 0 && !mBlockMeta.isNoCache()) {
        BlockStoreLocation loc = BlockStoreLocation.anyDirInTier(mStorageTierAssoc.getAlias(0));
        mLocalBlockStore.createBlock(mBlockMeta.getSessionId(), mBlockMeta.getBlockId(),
            AllocateOptions.forCreate(mInitialBlockSize, loc));
        mBlockWriter = mLocalBlockStore.getBlockWriter(
            mBlockMeta.getSessionId(), mBlockMeta.getBlockId());
      }
    } catch (BlockAlreadyExistsException e) {
      // This can happen when there are concurrent UFS readers who are all trying to cache to block.
      LOG.debug(
          "Failed to update block writer for UFS block [blockId: {}, ufsPath: {}, offset: {}]."
              + "Concurrent UFS readers may be caching the same block.",
          mBlockMeta.getBlockId(), mBlockMeta.getUnderFileSystemPath(), offset, e);
      mBlockWriter = null;
    } catch (IOException | AlluxioException e) {
      LOG.warn(
          "Failed to update block writer for UFS block [blockId: {}, ufsPath: {}, offset: {}]: {}",
          mBlockMeta.getBlockId(), mBlockMeta.getUnderFileSystemPath(), offset, e.toString());
      mBlockWriter = null;
    }
  }
}
