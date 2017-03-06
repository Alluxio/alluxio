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

/**
 * This class represents the metadata of a block that is in UFS.
 */
public final class UnderFileSystemBlockMeta {
  private final ConstMeta mConstMeta;

  /** The set of session IDs to be committed. */
  private boolean mCommitPending;

  /**
   * The constant metadata of this UFS block.
   */
  public static final class ConstMeta {
    public final long mSessionId;
    public final long mBlockId;
    public final String mUnderFileSystemPath;
    /** The offset in bytes of the first byte of the block in its corresponding UFS file. */
    public final long mOffset;
    /** The block size in bytes. */
    public final long mBlockSize;

    /**
     * Creates {@link UnderFileSystemBlockMeta.ConstMeta} from a {@link LockBlockTOptions}.
     *
     * @param sessionId the session ID
     * @param blockId the block ID
     * @param options the thrift lock options
     */
    public ConstMeta(long sessionId, long blockId, LockBlockTOptions options) {
      mSessionId = sessionId;
      mBlockId = blockId;
      mUnderFileSystemPath = options.getUfsPath();
      mOffset = options.getOffset();
      mBlockSize = options.getBlockSize();
    }
  }

  /**
   * Creates a {@link UnderFileSystemBlockMeta}.
   *
   * @param meta the constant metadata of this UFS block
   */
  public UnderFileSystemBlockMeta(ConstMeta meta) {
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
  public String getUnderFileSystemPath() {
    return mConstMeta.mUnderFileSystemPath;
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

}

