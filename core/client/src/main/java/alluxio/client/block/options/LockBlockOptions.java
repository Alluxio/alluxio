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
 * Method options for locking block.
 */
@NotThreadSafe
public final class LockBlockOptions {
  private String mUfsPath;
  private long mOffset;
  private long mBlockSize;
  private int mMaxUfsReadConcurrency;

  /**
   * @return the default {@link LockBlockOptions}
   */
  public static LockBlockOptions defaults() {
    return new LockBlockOptions();
  }

  private LockBlockOptions() {}

  public String getUfsPath() {
    return mUfsPath;
  }

  public long getOffset() {
    return mOffset;
  }

  public long getBlockSize() {
    return mBlockSize;
  }

  public int getMaxUfsReadConcurrency() {
    return mMaxUfsReadConcurrency;
  }

  public LockBlockOptions setUfsPath(String ufsPath) {
    mUfsPath = ufsPath;
    return this;
  }

  public LockBlockOptions setOffset(long offset) {
    mOffset = offset;
    return this;
  }

  public LockBlockOptions setBlockSize(long blockSize) {
    mBlockSize = blockSize;
    return this;
  }

  public LockBlockOptions setMaxUfsReadConcurrency(int maxUfsReadConcurrency) {
    mMaxUfsReadConcurrency = maxUfsReadConcurrency;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (! (o instanceof LockBlockOptions)) {
      return false;
    }
    LockBlockOptions that = (LockBlockOptions) o;
    return Objects.equal(mUfsPath, that.mUfsPath)
        && Objects.equal(mOffset, that.mOffset)
        && Objects.equal(mBlockSize, that.mBlockSize)
        && Objects.equal(mMaxUfsReadConcurrency, that.mMaxUfsReadConcurrency);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mUfsPath, mOffset, mBlockSize, mMaxUfsReadConcurrency);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("ufsPath", mUfsPath)
        .add("offset", mOffset)
        .add("blockSize", mBlockSize)
        .add("maxUfsReadConcurrency", mMaxUfsReadConcurrency).toString();
  }

  public LockBlockTOptions toThrift() {
    LockBlockTOptions options = new LockBlockTOptions();
    options.setUfsPath(mUfsPath);
    options.setOffset(mOffset);
    options.setBlockSize(mBlockSize);
    options.setMaxUfsReadConcurrency(mMaxUfsReadConcurrency);
    return options;
  }
}