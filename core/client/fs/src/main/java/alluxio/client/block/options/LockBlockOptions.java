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

package alluxio.client.block.options;

import alluxio.thrift.LockBlockTOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for locking blocks.
 */
@NotThreadSafe
public final class LockBlockOptions {
  private String mUfsPath;
  private long mOffset;
  private long mBlockSize;
  private int mMaxUfsReadConcurrency;
  private long mMountId;

  /**
   * @return the default {@link LockBlockOptions}
   */
  public static LockBlockOptions defaults() {
    return new LockBlockOptions();
  }

  /**
   * Creates a {@link LockBlockOptions} instance.
   */
  private LockBlockOptions() {}

  /**
   * @return the UFS path
   */
  public String getUfsPath() {
    return mUfsPath;
  }

  /**
   * @return the block offset in the UFS file
   */
  public long getOffset() {
    return mOffset;
  }

  /**
   * @return the UFS block size
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

  /**
   * @param ufsPath the UFS path to set
   * @return the updated options object
   */
  public LockBlockOptions setUfsPath(String ufsPath) {
    mUfsPath = ufsPath;
    return this;
  }

  /**
   * @param offset the UFS block offset to set
   * @return the updated options object
   */
  public LockBlockOptions setOffset(long offset) {
    mOffset = offset;
    return this;
  }

  /**
   * @param blockSize the UFS block size to set
   * @return the updated options object
   */
  public LockBlockOptions setBlockSize(long blockSize) {
    mBlockSize = blockSize;
    return this;
  }

  /**
   * @param maxUfsReadConcurrency the maximum UFS read concurrency
   * @return the updated options object
   */
  public LockBlockOptions setMaxUfsReadConcurrency(int maxUfsReadConcurrency) {
    mMaxUfsReadConcurrency = maxUfsReadConcurrency;
    return this;
  }

  /**
   * @param mountId the id of the mount
   * @return the updated options object
   */
  public LockBlockOptions setMountId(long mountId) {
    mMountId = mountId;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LockBlockOptions)) {
      return false;
    }
    LockBlockOptions that = (LockBlockOptions) o;
    return Objects.equal(mBlockSize, that.mBlockSize)
        && Objects.equal(mMaxUfsReadConcurrency, that.mMaxUfsReadConcurrency)
        && Objects.equal(mMountId, that.mMountId)
        && Objects.equal(mOffset, that.mOffset)
        && Objects.equal(mUfsPath, that.mUfsPath);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mBlockSize, mMaxUfsReadConcurrency, mMountId, mOffset, mUfsPath);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("blockSize", mBlockSize)
        .add("maxUfsReadConcurrency", mMaxUfsReadConcurrency)
        .add("mountId", mMountId)
        .add("offset", mOffset)
        .add("ufsPath", mUfsPath)
        .toString();
  }

  /**
   * Converts the object to a {@link LockBlockTOptions} object.
   *
   * @return the thrift lock block options
   */
  public LockBlockTOptions toThrift() {
    LockBlockTOptions options = new LockBlockTOptions();
    options.setBlockSize(mBlockSize);
    options.setMaxUfsReadConcurrency(mMaxUfsReadConcurrency);
    options.setMountId(mMountId);
    options.setOffset(mOffset);
    options.setUfsPath(mUfsPath);
    return options;
  }
}
