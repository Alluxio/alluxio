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

package alluxio.worker.block.options;

import alluxio.thrift.LockBlockTOptions;

import com.google.common.base.Objects;

/**
 * The options to open a UFS block.
 */
public final class OpenUfsBlockOptions {
  /** The UFS path. */
  private final String mUnderFileSystemPath;
  /** The offset in bytes of the first byte of the block in its corresponding UFS file. */
  private final long mOffset;
  /** The block size in bytes. */
  private final long mBlockSize;
  /** The maximum concurrent UFS reader on the UFS block allowed when opening the block. */
  private final int mMaxUfsReadConcurrency;
  /** The id of the mount of this file is mapped to. */
  private final long mMountId;

  /**
   * Creates an instance of {@link OpenUfsBlockOptions}.
   *
   * @param options the {@link LockBlockTOptions}
   */
  public OpenUfsBlockOptions(LockBlockTOptions options) {
    mUnderFileSystemPath = options.getUfsPath();
    mOffset = options.getOffset();
    mBlockSize = options.getBlockSize();
    mMaxUfsReadConcurrency = options.getMaxUfsReadConcurrency();
    mMountId = options.getMountId();
  }

  /**
   * @return the UFS path
   */
  public String getUnderFileSystemPath() {
    return mUnderFileSystemPath;
  }

  /**
   * @return the offset of the block in the UFS file
   */
  public long getOffset() {
    return mOffset;
  }

  /**
   * @return the block size in bytes
   */
  public long getBlockSize() {
    return mBlockSize;
  }

  /**
   * @return the maximum UFS read concurrency
   */
  public int getMaxUfsReadConcurrency() {
    return mMaxUfsReadConcurrency;
  }

  /**
   * @return the id of the mount
   */
  public long getMountId() {
    return mMountId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OpenUfsBlockOptions)) {
      return false;
    }
    OpenUfsBlockOptions that = (OpenUfsBlockOptions) o;
    return Objects.equal(mBlockSize, that.mBlockSize)
        && Objects.equal(mMaxUfsReadConcurrency, that.mMaxUfsReadConcurrency)
        && Objects.equal(mMountId, that.mMountId)
        && Objects.equal(mOffset, that.mOffset)
        && Objects.equal(mUnderFileSystemPath, that.mUnderFileSystemPath);
  }

  @Override
  public int hashCode() {
    return Objects
        .hashCode(mBlockSize, mMaxUfsReadConcurrency, mOffset, mMountId, mUnderFileSystemPath);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("blockSize", mBlockSize)
        .add("maxUfsReadConcurrency", mMaxUfsReadConcurrency)
        .add("mountId", mMountId)
        .add("offset", mOffset)
        .add("underFileSystemPath", mUnderFileSystemPath)
        .toString();
  }
}
