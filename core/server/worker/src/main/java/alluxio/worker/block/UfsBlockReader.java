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
import alluxio.Constants;
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
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final long FILE_BUFFER_SIZE = Configuration.getBytes(
      PropertyKey.WORKER_FILE_BUFFER_SIZE);
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();

  private final UfsBlockMeta mBlockMeta;
  private final boolean mNoCache;
  private final BlockStore mAlluxioBlockStore;

  private InputStream mUFSInputStream;
  private BlockWriter mBlockWriter;
  private boolean mClosed;

  /** The position within the block. */
  private long mPos;

  public UfsBlockReader(UfsBlockMeta blockMeta, long offset, boolean noCache,
      BlockStore alluxioBlockStore)
      throws BlockDoesNotExistException, IOException {
    mBlockMeta = blockMeta;
    UnderFileSystem ufs = UnderFileSystem.Factory.get(blockMeta.getUfsPath());
    ufs.connectFromWorker(
        NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.WORKER_RPC));
    if (!ufs.isFile(blockMeta.getUfsPath())) {
      throw new BlockDoesNotExistException(
          ExceptionMessage.UFS_PATH_DOES_NOT_EXIST.getMessage(blockMeta.getUfsPath()));
    }
    mAlluxioBlockStore = alluxioBlockStore;
    mNoCache = noCache;
    mPos = 0;

    updateUfsInputStream(offset);
    updateBlockWriter(offset);
    mPos = offset;
    mClosed = false;

    mBlockMeta.setBlockReader(this);
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
    if (mUFSInputStream != null) {
      while (bytesRead < bytesToRead) {
        int read = mUFSInputStream.read(data, bytesRead, (int) (bytesToRead - bytesRead));
        if (read == -1) {
          break;
        }
        bytesRead += read;
      }
    }

    // We should always read the number of bytes as expected since the UFS file length (hence block
    // size) should be always accurate.
    Preconditions.checkState(bytesRead == bytesRead,
        "Not enough bytes have been read [bytesRead: {}, bytesToRead: {}] from the UFS file: {}.",
        bytesRead, bytesToRead, mBlockMeta.getUfsPath());
    ByteBuffer buffer = ByteBuffer.wrap(data, 0, bytesRead);
    if (mBlockWriter != null) {
      mBlockWriter.append(buffer.duplicate());
    }
    mPos += bytesRead;
    return buffer;
  }

  @Override
  public int transferTo(ByteBuf buf) throws IOException {
    Preconditions.checkState(!mClosed);
    int bytesRead = 0;
    ByteBuf bufCopy = buf.duplicate();
    bufCopy.readerIndex(bufCopy.writerIndex());
    if (mUFSInputStream != null) {
      bytesRead = buf.writeBytes(mUFSInputStream, bytesRead);
    }

    if (bytesRead <= 0) {
      return bytesRead;
    }

    if (mBlockWriter != null) {
      while (bufCopy.readableBytes() > 0) {
        mBlockWriter.transferFrom(bufCopy);
      }
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
      if (mUFSInputStream != null) {
        closer.register(mUFSInputStream);
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

  private boolean isBlockCached() {
    return mBlockWriter != null && mPos == mBlockMeta.getBlockSize();
  }

  private void updateUfsInputStream(long offset) throws IOException {
    if (offset >= mBlockMeta.getBlockSize() || offset != mPos) {
      mUFSInputStream.close();
      mUFSInputStream = null;
    }

    if (mUFSInputStream == null && offset < mBlockMeta.getBlockSize()) {
      UnderFileSystem ufs = UnderFileSystem.Factory.get(mBlockMeta.getUfsPath());
      mUFSInputStream = ufs.open(mBlockMeta.getUfsPath(),
          OpenOptions.defaults().setOffset(mBlockMeta.getOffset() + offset));
    }
  }

  private void updateBlockWriter(long offset) {
    try {
      if (offset != mPos && mBlockWriter != null) {
        mBlockWriter.close();
        mBlockWriter = null;
        mAlluxioBlockStore.abortBlock(mBlockMeta.getSessionId(), mBlockMeta.getBlockId());
      }
      if (offset == 0 && !mNoCache) {
        BlockStoreLocation loc = BlockStoreLocation.anyDirInTier(mStorageTierAssoc.getAlias(0));
        String blockPath = mAlluxioBlockStore
            .createBlock(mBlockMeta.getSessionId(), mBlockMeta.getBlockId(), loc, FILE_BUFFER_SIZE)
            .getPath();
        mBlockWriter = new LocalFileBlockWriter(blockPath);
      }
    } catch (Exception e) {
      LOG.warn("Failed to update block writer for UFS block [blockId: {}, ufsPath: {}, offset: {}]",
          mBlockMeta.getBlockId(), mBlockMeta.getUfsPath(), offset, e);
      mBlockWriter = null;
    }
  }
}
