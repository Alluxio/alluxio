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

package alluxio.worker.block.meta;

import alluxio.thrift.LockBlockTOptions;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;

import java.io.IOException;

/**
 * This class represents the metadata of a UFS block.
 */
public final class UfsBlockMeta {
  private ConstMeta mConstMeta;

  /** The set of session IDs to be committed. */
  private boolean mCommitPending;
  private BlockReader mBlockReader;
  private BlockWriter mBlockWriter;

  /**
   * The constant metadata of this UFS block.
   */
  public static final class ConstMeta {
    public final long mSessionId;
    public final long mBlockId;
    public final String mUfsPath;
    /** The offset in bytes of the first byte of the block in its corresponding UFS file. */
    public final long mOffset;
    /** The block size in bytes. */
    public final long mBlockSize;

    /**
     * Creates {@link UfsBlockMeta.ConstMeta} from a {@link LockBlockTOptions}.
     *
     * @param sessionId the session ID
     * @param blockId the block ID
     * @param options the thrift lock options
     */
    public ConstMeta(long sessionId, long blockId, LockBlockTOptions options) {
      mSessionId = sessionId;
      mBlockId = blockId;
      mUfsPath = options.getUfsPath();
      mOffset = options.getOffset();
      mBlockSize = options.getBlockSize();
    }
  }

  /**
   * Creates a {@link UfsBlockMeta}.
   *
   * @param meta the constant metadata of this UFS block
   */
  public UfsBlockMeta(ConstMeta meta) {
    mConstMeta = meta;
  }

  /**
   * @return the session ID
   */
  public long getSessionId() {
    return mConstMeta.mSessionId;
  }

  /**
   * @return the block ID
   */
  public long getBlockId() {
    return mConstMeta.mBlockId;
  }

  /**
   * @return the UFS path
   */
  public String getUfsPath() {
    return mConstMeta.mUfsPath;
  }

  /**
   * @return the offset of the block in the UFS file
   */
  public long getOffset() {
    return mConstMeta.mOffset;
  }

  /**
   * @return the block size in bytes
   */
  public long getBlockSize() {
    return mConstMeta.mBlockSize;
  }

  /**
   * @return the cached the block reader if it is not closed
   */
  public BlockReader getBlockReader() {
    if (mBlockReader != null && mBlockReader.isClosed()) {
      mBlockReader = null;
    }
    return mBlockReader;
  }

  /**
   * @return the block writer
   */
  public BlockWriter getBlockWriter() {
    return mBlockWriter;
  }

  /**
   * @return true if the block is pending to be committed in the Alluxio block store
   */
  public boolean getCommitPending() {
    return mCommitPending;
  }

  /**
   * @param commitPending set to true if the block is pending to be committed
   */
  public void setCommitPending(boolean commitPending) {
    mCommitPending = commitPending;
  }

  /**
   * @param blockReader the block reader to be set
   */
  public void setBlockReader(BlockReader blockReader) {
    mBlockReader = blockReader;
  }

  /**
   * @param blockWriter the block writer to be set
   */
  public void setBlockWriter(BlockWriter blockWriter) {
    mBlockWriter = blockWriter;
  }

  /**
   * Closes the block reader or writer.
   *
   * @throws IOException if it fails to close block reader or writer
   */
  public void closeReaderOrWriter() throws IOException {
    if (mBlockReader != null) {
      mBlockReader.close();
      mBlockReader = null;
    }
    if (mBlockWriter != null) {
      mBlockWriter.close();
      mBlockWriter = null;
    }
  }
}

