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
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
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
 * This class implements a {@link BlockReader} to read a block directly from UFS, and optionally
 * cache the block to Alluxio worker if the whole block it read.
 */
@NotThreadSafe
public final class UnderFileSystemBlockReader implements BlockReader {
  private static final Logger LOG = LoggerFactory.getLogger(UnderFileSystemBlockReader.class);

  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();

  /** The file buffer size used to allocate memory from Alluxio storage. */
  private final long mFileBufferSize;
  /** The block metadata for the UFS block. */
  private final UnderFileSystemBlockMeta mBlockMeta;
  /** If set, do not cache the block. */
  private final boolean mNoCache;
  /** The Alluxio block store. It is used to interact with Alluxio. */
  private final BlockStore mAlluxioBlockStore;

  /** The input stream to read from UFS. */
  private InputStream mUnderFileSystemInputStream;
  /** The block writer to write the block to Alluxio. */
  private BlockWriter mBlockWriter;
  /** If set, the reader is closed and should not be used afterwards. */
  private boolean mClosed;

  /** The position of mUnderFileSystemInputStream (if not null) is blockStart + mPos. */
  private long mInStreamPos;
  /** The position of mBlockWriter if not null is mPos. */
  private long mBlockWriterPos;

  /**
   * Creates an instance of {@link UnderFileSystemBlockReader} and initializes it with a reading
   * offset.
   *
   * @param blockMeta the block meta
   * @param offset the position within the block to start the read
   * @param noCache do not cache the block
   * @param alluxioBlockStore the Alluxio block store
   * @return the block reader
   * @throws BlockDoesNotExistException if the UFS block does not exist in the UFS block store
   * @throws IOException if an I/O related error occur
   */
  public static UnderFileSystemBlockReader create(UnderFileSystemBlockMeta blockMeta, long offset,
      boolean noCache, BlockStore alluxioBlockStore)
      throws BlockDoesNotExistException, IOException {
    UnderFileSystemBlockReader ufsBlockReader =
        new UnderFileSystemBlockReader(blockMeta, noCache, alluxioBlockStore);
    ufsBlockReader.init(offset);
    return ufsBlockReader;
  }

  /**
   * Creates an instance of {@link UnderFileSystemBlockReader}.
   *
   * @param blockMeta the block meta
   * @param noCache do not cache the block
   * @param alluxioBlockStore the Alluxio block store
   */
  private UnderFileSystemBlockReader(UnderFileSystemBlockMeta blockMeta, boolean noCache,
      BlockStore alluxioBlockStore) {
    mFileBufferSize = Configuration.getBytes(PropertyKey.WORKER_FILE_BUFFER_SIZE);
    mBlockMeta = blockMeta;
    mAlluxioBlockStore = alluxioBlockStore;
    mNoCache = noCache;
    mInStreamPos = -1;
    mBlockWriterPos = -1;
    mBlockMeta.setBlockReader(this);
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
    updateBlockWriter(offset);
    updateUnderFileSystemInputStream(offset);

    long bytesToRead = Math.min(length, mBlockMeta.getBlockSize() - offset);
    if (bytesToRead <= 0) {
      return null;
    }
    byte[] data = new byte[(int) bytesToRead];
    int bytesRead = 0;
    if (mUnderFileSystemInputStream != null) {
      while (bytesRead < bytesToRead) {
        int read =
            mUnderFileSystemInputStream.read(data, bytesRead, (int) (bytesToRead - bytesRead));
        if (read == -1) {
          break;
        }
        bytesRead += read;
      }
    }
    mInStreamPos += bytesRead;

    // We should always read the number of bytes as expected since the UFS file length (hence block
    // size) should be always accurate.
    Preconditions.checkState(bytesRead == bytesToRead,
        "Not enough bytes have been read [bytesRead: {}, bytesToRead: {}] from the UFS file: {}.",
        bytesRead, bytesToRead, mBlockMeta.getUnderFileSystemPath());
    if (mBlockWriter != null && mBlockWriterPos < mInStreamPos) {
      Preconditions.checkState(mBlockWriterPos >= offset);
      ByteBuffer buffer = ByteBuffer
          .wrap(data, (int) (mBlockWriterPos - offset), (int) (mInStreamPos - mBlockWriterPos));
      mBlockWriter.append(buffer.duplicate());
      mBlockWriterPos = mInStreamPos;
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
    int bytesRead = 0;
    ByteBuf bufCopy = buf.duplicate();
    bufCopy.readerIndex(bufCopy.writerIndex());
    bytesRead = buf.writeBytes(mUnderFileSystemInputStream, buf.writableBytes());

    if (bytesRead <= 0) {
      return bytesRead;
    }

    mInStreamPos += bytesRead;

    bufCopy.writerIndex(buf.writerIndex());
    if (mBlockWriter != null) {
      while (bufCopy.readableBytes() > 0) {
        mBlockWriter.transferFrom(bufCopy);
      }
      mBlockWriterPos += bytesRead;
    }

    return bytesRead;
  }

  /**
   * Closes the block reader. After this, this block reader should not be used again.
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

      boolean isBlockCached = isBlockCached();
      Closer closer = Closer.create();
      if (mBlockWriter != null) {
        closer.register(mBlockWriter);
      }
      if (mUnderFileSystemInputStream != null) {
        closer.register(mUnderFileSystemInputStream);
      }
      closer.close();

      if (isBlockCached) {
        mBlockMeta.setCommitPending(true);
      }
    } finally {
      mClosed = true;
    }
  }

  @Override
  public boolean isClosed() {
    return mClosed;
  }

  /**
   * @return true if the whole block is read and cached to the temporary block location
   */
  private boolean isBlockCached() {
    return mBlockWriter != null && mBlockWriterPos == mBlockMeta.getBlockSize();
  }

  /**
   * Updates the UFS input stream given an offset to read.
   *
   * @param offset the read offset
   * @throws IOException any I/O errors occur while updating the input stream
   */
  private void updateUnderFileSystemInputStream(long offset) throws IOException {
    if ((mUnderFileSystemInputStream != null) && (offset >= mBlockMeta.getBlockSize()
        || offset != mInStreamPos)) {
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
   * @throws IOException any I/O errors occur while updating the input stream
   */
  private void updateBlockWriter(long offset) {
    try {
      if (mBlockWriter != null && offset > mBlockWriterPos) {
        mBlockWriter.close();
        mBlockWriter = null;
        mAlluxioBlockStore.abortBlock(mBlockMeta.getSessionId(), mBlockMeta.getBlockId());
        mBlockWriterPos = -1;
      }
      if (mBlockWriter == null && offset == 0 && !mNoCache) {
        BlockStoreLocation loc = BlockStoreLocation.anyDirInTier(mStorageTierAssoc.getAlias(0));
        String blockPath = mAlluxioBlockStore
            .createBlock(mBlockMeta.getSessionId(), mBlockMeta.getBlockId(), loc, mFileBufferSize)
            .getPath();
        mBlockWriter = new LocalFileBlockWriter(blockPath);
        mBlockWriterPos = 0;
      }
    } catch (Exception e) {
      // This can happen when there are concurrent UFS readers.
      LOG.debug(
          "Failed to update block writer for UFS block [blockId: {}, ufsPath: {}, offset: {}]",
          mBlockMeta.getBlockId(), mBlockMeta.getUnderFileSystemPath(), offset, e);
      mBlockWriter = null;
    }
  }
}
