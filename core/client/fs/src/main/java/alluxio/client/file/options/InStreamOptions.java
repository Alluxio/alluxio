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

package alluxio.client.file.options;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.client.AlluxioStorageType;
import alluxio.client.ReadType;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.CreateOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.util.CommonUtils;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for reading a file.
 */
@PublicApi
@NotThreadSafe
public final class InStreamOptions {
  private FileWriteLocationPolicy mCacheLocationPolicy;
  private ReadType mReadType;
  /** Cache incomplete blocks if Alluxio is configured to store blocks in Alluxio storage. */
  private boolean mCachePartiallyReadBlock;
  /**
   * The cache read buffer size in seek. This is only used if {@link #mCachePartiallyReadBlock}
   * is enabled.
   */
  private long mSeekBufferSizeBytes;
  /** The maximum UFS read concurrency for one block on one Alluxio worker. */
  private int mMaxUfsReadConcurrency;
  /** The location policy to determine the worker location to serve UFS block reads. */
  private BlockLocationPolicy mUfsReadLocationPolicy;

  /**
   * @return the default {@link InStreamOptions}
   */
  public static InStreamOptions defaults() {
    return new InStreamOptions();
  }

  private InStreamOptions() {
    mReadType = Configuration.getEnum(PropertyKey.USER_FILE_READ_TYPE_DEFAULT, ReadType.class);
    try {
      mCacheLocationPolicy = CommonUtils.createNewClassInstance(
          Configuration.<FileWriteLocationPolicy>getClass(
              PropertyKey.USER_FILE_WRITE_LOCATION_POLICY), new Class[] {}, new Object[] {});
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    CreateOptions blockLocationPolicyCreateOptions = CreateOptions.defaults()
        .setLocationPolicyClassName(
            Configuration.get(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY))
        .setDeterministicHashPolicyNumShards(Configuration
            .getInt(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS));
    mUfsReadLocationPolicy = BlockLocationPolicy.Factory.create(blockLocationPolicyCreateOptions);
    mCachePartiallyReadBlock =
        Configuration.getBoolean(PropertyKey.USER_FILE_CACHE_PARTIALLY_READ_BLOCK);
    mSeekBufferSizeBytes = Configuration.getBytes(PropertyKey.USER_FILE_SEEK_BUFFER_SIZE_BYTES);
    mMaxUfsReadConcurrency =
        Configuration.getInt(PropertyKey.USER_UFS_BLOCK_READ_CONCURRENCY_MAX);
  }

  /**
   * @return the location policy to use when storing data to Alluxio
   * @deprecated since version 1.5 and will be removed in version 2.0. Use
   *             {@link InStreamOptions#getCacheLocationPolicy()}.
   */
  @Deprecated
  public FileWriteLocationPolicy getLocationPolicy() {
    return mCacheLocationPolicy;
  }

  /**
   * @return the location policy to use when storing data to Alluxio
   */
  public FileWriteLocationPolicy getCacheLocationPolicy() {
    return mCacheLocationPolicy;
  }

  /**
   * @return the Alluxio storage type
   */
  public AlluxioStorageType getAlluxioStorageType() {
    return mReadType.getAlluxioStorageType();
  }

  /**
   * @return the maximum UFS read concurrency
   */
  public int getMaxUfsReadConcurrency() {
    return mMaxUfsReadConcurrency;
  }

  /**
   * @return the UFS read location policy
   */
  public BlockLocationPolicy getUfsReadLocationPolicy() {
    return mUfsReadLocationPolicy;
  }

  /**
   * @param policy the location policy to use when storing data to Alluxio
   * @return the updated options object
   * @deprecated since version 1.5 and will be removed in version 2.0. Use
   *             {@link InStreamOptions#setCacheLocationPolicy(FileWriteLocationPolicy)}.
   */
  @Deprecated
  public InStreamOptions setLocationPolicy(FileWriteLocationPolicy policy) {
    mCacheLocationPolicy = policy;
    return this;
  }

  /**
   * @param policy the location policy to use when storing data to Alluxio
   * @return the updated options object
   */
  public InStreamOptions setCacheLocationPolicy(FileWriteLocationPolicy policy) {
    mCacheLocationPolicy = policy;
    return this;
  }

  /**
   * Sets the {@link ReadType}.
   *
   * @param readType the {@link ReadType} for this operation. Setting this will override the
   *        {@link AlluxioStorageType}.
   * @return the updated options object
   */
  public InStreamOptions setReadType(ReadType readType) {
    mReadType = readType;
    return this;
  }

  /**
   * @param maxUfsReadConcurrency the maximum UFS read concurrency
   * @return the updated options object
   */
  public InStreamOptions setMaxUfsReadConcurrency(int maxUfsReadConcurrency) {
    mMaxUfsReadConcurrency = maxUfsReadConcurrency;
    return this;
  }

  /**
   * @return true if incomplete block caching is enabled
   */
  public boolean isCachePartiallyReadBlock() {
    return mCachePartiallyReadBlock;
  }

  /**
   * Enables/Disables incomplete block caching.
   *
   * @param cachePartiallyReadBlock set to true if to enable incomplete block caching
   * @return the updated options object
   */
  public InStreamOptions setCachePartiallyReadBlock(boolean cachePartiallyReadBlock) {
    mCachePartiallyReadBlock = cachePartiallyReadBlock;
    return this;
  }

  /**
   * @return the seek buffer size in bytes
   */
  public long getSeekBufferSizeBytes() {
    return mSeekBufferSizeBytes;
  }

  /**
   * Sets {@link #mSeekBufferSizeBytes}.
   *
   * @param bufferSizeBytes the seek buffer size
   * @return the updated options object
   */
  public InStreamOptions setSeekBufferSizeBytes(long bufferSizeBytes) {
    mSeekBufferSizeBytes = bufferSizeBytes;
    return this;
  }

  /**
   * Sets the UFS read location policy.
   *
   * @param policy the UFS read location policy
   * @return the updated options object
   */
  public InStreamOptions setUfsReadLocationPolicy(BlockLocationPolicy policy) {
    mUfsReadLocationPolicy = policy;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InStreamOptions)) {
      return false;
    }
    InStreamOptions that = (InStreamOptions) o;
    return Objects.equal(mCacheLocationPolicy, that.mCacheLocationPolicy)
        && Objects.equal(mReadType, that.mReadType)
        && Objects.equal(mCachePartiallyReadBlock, that.mCachePartiallyReadBlock)
        && Objects.equal(mSeekBufferSizeBytes, that.mSeekBufferSizeBytes)
        && Objects.equal(mMaxUfsReadConcurrency, that.mMaxUfsReadConcurrency)
        && Objects.equal(mUfsReadLocationPolicy, that.mUfsReadLocationPolicy);
  }

  @Override
  public int hashCode() {
    return Objects
        .hashCode(
            mCacheLocationPolicy,
            mReadType,
            mCachePartiallyReadBlock,
            mSeekBufferSizeBytes,
            mMaxUfsReadConcurrency,
            mUfsReadLocationPolicy);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("cacheLocationPolicy", mCacheLocationPolicy)
        .add("readType", mReadType).add("cachePartiallyReadBlock", mCachePartiallyReadBlock)
        .add("seekBufferSize", mSeekBufferSizeBytes)
        .add("maxUfsReadConcurrency", mMaxUfsReadConcurrency)
        .add("ufsReadLocationPolicy", mUfsReadLocationPolicy).toString();
  }
}
