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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.LocalFileBlockWriter;
import alluxio.worker.block.meta.UnderFileSystemBlockMeta;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
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
public final class UnderFileSystemBlockReader implements BlockReader {
  private static final Logger LOG = LoggerFactory.getLogger(UnderFileSystemBlockReader.class);

  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();

  /** The initial size of the block allocated in Alluxio storage when the block is cached. */
  private final long mInitialBlockSize;
  /** The block metadata for the UFS block. */
  private final UnderFileSystemBlockMeta mBlockMeta;
  /** If set, do not cache the block. */
  private final boolean mNoCache;
  /** The Local block store. It is used to interact with Alluxio. */
  private final BlockStore mLocalBlockStore;

  /** The input stream to read from UFS. */
  private InputStream mUnderFileSystemInputStream;
  /** The block writer to write the block to Alluxio. */
  private LocalFileBlockWriter mBlockWriter;
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
   * @param noCache do not cache the block if set
   * @param localBlockStore the Local block store
   * @return the block reader
   * @throws BlockDoesNotExistException if the UFS block does not exist in the UFS block store
   * @throws IOException if an I/O related error occur
   */
  public static UnderFileSystemBlockReader create(UnderFileSystemBlockMeta blockMeta, long offset,
      boolean noCache, BlockStore localBlockStore)
      throws BlockDoesNotExistException, IOException {
    UnderFileSystemBlockReader ufsBlockReader =
        new UnderFileSystemBlockReader(blockMeta, noCache, localBlockStore);
    ufsBlockReader.init(offset);
    return ufsBlockReader;
  }

  /**
   * Creates an instance of {@link UnderFileSystemBlockReader}.
   *
   * @param blockMeta the block meta
   * @param noCache do not cache the block
   * @param localBlockStore the Local block store
   */
  private UnderFileSystemBlockReader(UnderFileSystemBlockMeta blockMeta, boolean noCache,
      BlockStore localBlockStore) {
    mInitialBlockSize = Configuration.getBytes(PropertyKey.WORKER_FILE_BUFFER_SIZE);
    mBlockMeta = blockMeta;
    mLocalBlockStore = localBlockStore;
    mNoCache = noCache;
    mInStreamPos = -1;
  }

  /**
   * Initializes the reader. This is only called in the factory method.
   *
   * @param offset the position within the block to start the read
   * @throws BlockDoesNotExistException if the UFS block does not exist in the UFS block store
   * @throws IOException if an I/O related error occur
   */
  private void init(long offset) throws BlockDoesNotExistException, IOException {
    UnderFileSystem ufs = UnderFileSystem.Factory.get(mBlockMeta.getUnderFileSystemPath());
    ufs.connectFromWorker(
        NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.WORKER_RPC));
    if (!ufs.isFile(mBlockMeta.getUnderFileSystemPath())) {
      throw new BlockDoesNotExistException(
          ExceptionMessage.UFS_PATH_DOES_NOT_EXIST.getMessage(mBlockMeta.getUnderFileSystemPath()));
    }

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
    Preconditions.checkNotNull(mUnderFileSystemInputStream);
    while (bytesRead < bytesToRead) {
      int read = mUnderFileSystemInputStream.read(data, bytesRead, (int) (bytesToRead - bytesRead));
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
      Preconditions.checkState(mBlockWriter.getPosition() >= offset);
      ByteBuffer buffer = ByteBuffer.wrap(data, (int) (mBlockWriter.getPosition() - offset),
          (int) (mInStreamPos - mBlockWriter.getPosition()));
      mBlockWriter.append(buffer.duplicate());
    }
    return ByteBuffer.wrap(data, 0, bytesRead);
  }

  /**
   * This interface is supposed to be used for sequence block reads.
   *
   * @param buf the byte buffer
   * @return the number of bytes read, -1 if it reaches EOF and none was read
   * @throws IOException if any I/O errors occur when reading the block
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
        (int) Math.min((long) buf.writableBytes(), mBlockMeta.getBlockSize() - mInStreamPos);
    int bytesRead = buf.writeBytes(mUnderFileSystemInputStream, bytesToRead);

    if (bytesRead <= 0) {
      return bytesRead;
    }

    mInStreamPos += bytesRead;

    if (mBlockWriter != null) {
      bufCopy.writerIndex(buf.writerIndex());
      while (bufCopy.readableBytes() > 0) {
        mBlockWriter.transferFrom(bufCopy);
      }
    }

    return bytesRead;
  }

  /**
   * Closes the block reader. After this, this block reader should not be used anymore.
   * This is recommended to be called after the client finishes reading the block. It is usually
   * triggered when the client unlocks the block.
   *
   * @throws IOException if any I/O errors occur
   */
  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }

    try {
      // This aborts the block if the block is not fully read.
      updateBlockWriter(mBlockMeta.getBlockSize());

      Closer closer = Closer.create();
      if (mBlockWriter != null) {
        closer.register(mBlockWriter);
      }
      if (mUnderFileSystemInputStream != null) {
        closer.register(mUnderFileSystemInputStream);
      }
      closer.close();
    } finally {
      mClosed = true;
    }
  }

  @Override
  public boolean isClosed() {
    return mClosed;
  }

  /**
   * Updates the UFS input stream given an offset to read.
   *
   * @param offset the read offset within the block
   * @throws IOException any I/O errors occur while updating the input stream
   */
  private void updateUnderFileSystemInputStream(long offset) throws IOException {
    if ((mUnderFileSystemInputStream != null) && offset != mInStreamPos) {
      mUnderFileSystemInputStream.close();
      mUnderFileSystemInputStream = null;
      mInStreamPos = -1;
    }

    if (mUnderFileSystemInputStream == null && offset < mBlockMeta.getBlockSize()) {
      UnderFileSystem ufs = UnderFileSystem.Factory.get(mBlockMeta.getUnderFileSystemPath());
      mUnderFileSystemInputStream = ufs.open(mBlockMeta.getUnderFileSystemPath(),
          OpenOptions.defaults().setOffset(mBlockMeta.getOffset() + offset));
      mInStreamPos = offset;
    }
  }

  /**
   * Updates the block writer given an offset to read. If the offset is beyond the current
   * position of the block writer, the block writer will be aborted.
   *
   * @param offset the read offset
   */
  private void updateBlockWriter(long offset) throws IOException {
    try {
      if (mBlockWriter != null && offset > mBlockWriter.getPosition()) {
        mBlockWriter.close();
        mBlockWriter = null;
        mLocalBlockStore.abortBlock(mBlockMeta.getSessionId(), mBlockMeta.getBlockId());
      }
    } catch (BlockDoesNotExistException e) {
      // This can only happen when the session is expired.
      LOG.warn("Block {} does not exist when being aborted.", mBlockMeta.getBlockId());
    } catch (BlockAlreadyExistsException | InvalidWorkerStateException | IOException e) {
      // We cannot skip the exception here because we need to make sure that the user of this
      // reader does not commit the block if it fails to abort the block.
      throw CommonUtils.castToIOException(e);
    }
    try {
      if (mBlockWriter == null && offset == 0 && !mNoCache) {
        BlockStoreLocation loc = BlockStoreLocation.anyDirInTier(mStorageTierAssoc.getAlias(0));
        String blockPath = mLocalBlockStore
            .createBlock(mBlockMeta.getSessionId(), mBlockMeta.getBlockId(), loc,
                mInitialBlockSize).getPath();
        mBlockWriter = new LocalFileBlockWriter(blockPath);
      }
    } catch (IOException | BlockAlreadyExistsException | WorkerOutOfSpaceException e) {
      // This can happen when there are concurrent UFS readers who are all trying to cache to block.
      LOG.debug(
          "Failed to update block writer for UFS block [blockId: {}, ufsPath: {}, offset: {}]",
          mBlockMeta.getBlockId(), mBlockMeta.getUnderFileSystemPath(), offset, e);
      mBlockWriter = null;
    }
  }
}
