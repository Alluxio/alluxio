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

package alluxio.file.options;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a file.
 *
 * @param <T> the type of the concrete subclass
 */
@NotThreadSafe
public abstract class CreateFileOptions<T extends CreateFileOptions<T>>
    extends CreatePathOptions<T> {
  protected long mBlockSizeBytes;
  protected boolean mCacheable;
  protected int mReplicationDurable;
  protected int mReplicationMax;
  protected int mReplicationMin;

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @param blockSizeBytes the block size to use
   * @return the updated options object
   */
  public T setBlockSizeBytes(long blockSizeBytes) {
    mBlockSizeBytes = blockSizeBytes;
    return getThis();
  }

  /**
   * @return true if file is cacheable
   */
  public boolean isCacheable() {
    return mCacheable;
  }

  /**
   * @param cacheable true if the file is cacheable, false otherwise
   * @return the updated options object
   */
  public T setCacheable(boolean cacheable) {
    mCacheable = cacheable;
    return getThis();
  }

  /**
   * @return the number of block replication for durable write
   */
  public int getReplicationDurable() {
    return mReplicationDurable;
  }

  /**
   * @return the maximum number of block replication
   */
  public int getReplicationMax() {
    return mReplicationMax;
  }

  /**
   * @return the minimum number of block replication
   */
  public int getReplicationMin() {
    return mReplicationMin;
  }

  /**
   * @param replicationDurable the number of block replication for durable write
   * @return the updated options object
   */
  public CreateFileOptions setReplicationDurable(int replicationDurable) {
    mReplicationDurable = replicationDurable;
    return this;
  }

  /**
   * @param replicationMax the maximum number of block replication
   * @return the updated options object
   */
  public CreateFileOptions setReplicationMax(int replicationMax) {
    com.google.common.base.Preconditions
            .checkArgument(
                    replicationMax == alluxio.Constants.REPLICATION_MAX_INFINITY || replicationMax >= 0,
                    alluxio.exception.PreconditionMessage.INVALID_REPLICATION_MAX_VALUE);
    mReplicationMax = replicationMax;
    return this;
  }

  /**
   * @param replicationMin the minimum number of block replication
   * @return the updated options object
   */
  public CreateFileOptions setReplicationMin(int replicationMin) {
    com.google.common.base.Preconditions.checkArgument(replicationMin >= 0,
            alluxio.exception.PreconditionMessage.INVALID_REPLICATION_MIN_VALUE);
    mReplicationMin = replicationMin;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateFileOptions)) {
      return false;
    }
    if (!(super.equals(o))) {
      return false;
    }
    CreateFileOptions that = (CreateFileOptions) o;
    return Objects.equal(mBlockSizeBytes, that.mBlockSizeBytes)
        && Objects.equal(mCacheable, that.mCacheable);
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hashCode(mBlockSizeBytes, mCacheable);
  }

  @Override
  public String toString() {
    return toStringHelper()
        .add("blockSizeBytes", mBlockSizeBytes)
        .add("cacheable", mCacheable)
        .toString();
  }
}
