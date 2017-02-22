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
import alluxio.worker.block.meta.UfsBlockMeta;

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
 * This class provides read access to a block data file stored in the UFS. It also caches the data
 * to a local block if it tries to read the whole block.
 */
@NotThreadSafe
public final class UfsBlockReader implements BlockReader {
  private static final Logger LOG = LoggerFactory.getLogger(UfsBlockReader.class);

  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();

  private final long mFileBufferSize;
  private final UfsBlockMeta mBlockMeta;
  private final boolean mNoCache;
  private final BlockStore mAlluxioBlockStore;

  private InputStream mUfsInputStream;
  private BlockWriter mBlockWriter;
  private boolean mClosed;

  /** The position of mUfsInputStream (if not null) is blockStart + mPos. */
  private long mInStreamPos;
  /** The position of mBlockWriter if not null is mPos. */
  private long mBlockWriterPos;

  /**
   * Creates an instance of {@link UfsBlockReader} and initializes it with a reading offset.
   *
   * @param blockMeta the block meta
   * @param offset the position within the block to start the read
   * @param noCache do not cache the block
   * @param alluxioBlockStore the Alluxio block store
   * @return the block reader
   * @throws BlockDoesNotExistException if the UFS block does not exist in the UFS block store
   * @throws IOException if an I/O related error occur
   */
  public static UfsBlockReader create(UfsBlockMeta blockMeta, long offset, boolean noCache,
      BlockStore alluxioBlockStore) throws BlockDoesNotExistException, IOException {
    UfsBlockReader ufsBlockReader =
        new UfsBlockReader(blockMeta, noCache, alluxioBlockStore);
    ufsBlockReader.init(offset);
    return ufsBlockReader;
  }

  /**
   * Creates an instance of {@link UfsBlockReader}.
   *
   * @param blockMeta the block meta
   * @param noCache do not cache the block
   * @param alluxioBlockStore the Alluxio block store
   */
  private UfsBlockReader(UfsBlockMeta blockMeta, boolean noCache, BlockStore alluxioBlockStore) {
    mFileBufferSize = Configuration.getBytes(PropertyKey.WORKER_FILE_BUFFER_SIZE);
    mBlockMeta = blockMeta;
    mAlluxioBlockStore = alluxioBlockStore;
    mNoCache = noCache;
    mInStreamPos = -1;
    mBlockWriterPos = -1;
    mBlockMeta.setBlockReader(this);
  }

  /**
   * Initializes the reader.
   *
   * @param offset the position within the block to start the read
   * @throws BlockDoesNotExistException if the UFS block does not exist in the UFS block store
   * @throws IOException if an I/O related error occur
   */
  private void init(long offset) throws BlockDoesNotExistException, IOException {
    UnderFileSystem ufs = UnderFileSystem.Factory.get(mBlockMeta.getUfsPath());
    ufs.connectFromWorker(
        NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.WORKER_RPC));
    if (!ufs.isFile(mBlockMeta.getUfsPath())) {
      throw new BlockDoesNotExistException(
          ExceptionMessage.UFS_PATH_DOES_NOT_EXIST.getMessage(mBlockMeta.getUfsPath()));
    }

    updateUfsInputStream(offset);
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
    updateUfsInputStream(offset);

    long bytesToRead = Math.min(length, mBlockMeta.getBlockSize() - offset);
    if (bytesToRead <= 0) {
      return null;
    }
    byte[] data = new byte[(int) bytesToRead];
    int bytesRead = 0;
    if (mUfsInputStream != null) {
      while (bytesRead < bytesToRead) {
        int read = mUfsInputStream.read(data, bytesRead, (int) (bytesToRead - bytesRead));
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
        bytesRead, bytesToRead, mBlockMeta.getUfsPath());
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
    if (mUfsInputStream == null) {
      return -1;
    }
    int bytesRead = 0;
    ByteBuf bufCopy = buf.duplicate();
    bufCopy.readerIndex(bufCopy.writerIndex());
    bytesRead = buf.writeBytes(mUfsInputStream, buf.writableBytes());

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
   * This closes the block reader. After this, this block reader should not be used again.
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
      if (mUfsInputStream != null) {
        closer.register(mUfsInputStream);
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
  private void updateUfsInputStream(long offset) throws IOException {
    if ((mUfsInputStream != null) && (offset >= mBlockMeta.getBlockSize()
        || offset != mInStreamPos)) {
      mUfsInputStream.close();
      mUfsInputStream = null;
      mInStreamPos = -1;
    }

    if (mUfsInputStream == null && offset < mBlockMeta.getBlockSize()) {
      UnderFileSystem ufs = UnderFileSystem.Factory.get(mBlockMeta.getUfsPath());
      mUfsInputStream = ufs.open(mBlockMeta.getUfsPath(),
          OpenOptions.defaults().setOffset(mBlockMeta.getOffset() + offset));
      mInStreamPos = offset;
    }
  }

  /**
   * Updates the block writer given an offset to read. The offset is beyond the current
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
          mBlockMeta.getBlockId(), mBlockMeta.getUfsPath(), offset, e);
      mBlockWriter = null;
    }
  }
}
