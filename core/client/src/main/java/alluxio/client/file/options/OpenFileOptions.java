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
import alluxio.client.ReadType;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.CreateOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.util.CommonUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for opening a file for reading.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class OpenFileOptions {
  private FileWriteLocationPolicy mCacheLocationPolicy;
  private ReadType mReadType;
  /** The maximum UFS read concurrency for one block on one Alluxio worker. */
  private int mMaxUfsReadConcurrency;
  /** The location policy to determine the worker location to serve UFS block reads. */
  private BlockLocationPolicy mUfsReadLocationPolicy;

  /**
   * @return the default {@link InStreamOptions}
   */
  public static OpenFileOptions defaults() {
    return new OpenFileOptions();
  }

  /**
   * Creates a new instance with defaults based on the configuration.
   */
  private OpenFileOptions() {
    mReadType =
        Configuration.getEnum(PropertyKey.USER_FILE_READ_TYPE_DEFAULT, ReadType.class);
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
    mMaxUfsReadConcurrency =
        Configuration.getInt(PropertyKey.USER_UFS_BLOCK_READ_CONCURRENCY_MAX);
  }

  /**
   * @return the location policy used when storing data to Alluxio
   * @deprecated since version 1.5 and will be removed in version 2.0. Use
   *             {@link OpenFileOptions#getCacheLocationPolicy()}.
   */
  @JsonIgnore
  @Deprecated
  public FileWriteLocationPolicy getLocationPolicy() {
    return mCacheLocationPolicy;
  }

  /**
   * @return the location policy used when storing data to Alluxio
   */
  @JsonIgnore
  public FileWriteLocationPolicy getCacheLocationPolicy() {
    return mCacheLocationPolicy;
  }

  /**
   * @return the location policy class used when storing data to Alluxio
   * @deprecated since version 1.5 and will be removed in version 2.0. Use
   *             {@link OpenFileOptions#getCacheLocationPolicyClass()}.
   */
  @Deprecated
  public String getLocationPolicyClass() {
    return mCacheLocationPolicy.getClass().getCanonicalName();
  }

  /**
   * @return the location policy class used when storing data to Alluxio
   */
  public String getCacheLocationPolicyClass() {
    return mCacheLocationPolicy.getClass().getCanonicalName();
  }

  /**
   * @return the read type
   */
  public ReadType getReadType() {
    return mReadType;
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
   * @return the location policy class name of the UFS read location policy
   */
  public String getUfsReadLocationPolicyClass() {
    return mUfsReadLocationPolicy.getClass().getCanonicalName();
  }

  /**
   * @param locationPolicy the location policy to use when storing data to Alluxio
   * @return the updated options object
   * @deprecated since version 1.5 and will be removed in version 2.0. Use
   *             {@link OpenFileOptions#setCacheLocationPolicy(FileWriteLocationPolicy)}.
   */
  @JsonIgnore
  @Deprecated
  public OpenFileOptions setLocationPolicy(FileWriteLocationPolicy locationPolicy) {
    mCacheLocationPolicy = locationPolicy;
    return this;
  }

  /**
   * @param locationPolicy the location policy to use when storing data to Alluxio
   * @return the updated options object
   */
  @JsonIgnore
  public OpenFileOptions setCacheLocationPolicy(FileWriteLocationPolicy locationPolicy) {
    mCacheLocationPolicy = locationPolicy;
    return this;
  }

  /**
   * @param policy the block location policy for the UFS read
   * @return the UFS read location policy
   */
  @JsonIgnore
  public OpenFileOptions setUfsReadLocationPolicy(BlockLocationPolicy policy) {
    mUfsReadLocationPolicy = policy;
    return this;
  }

  /**
   * @param className the location policy class to use when storing data to Alluxio
   * @return the updated options object
   * @deprecated since version 1.5 and will be removed in version 2.0. Use
   *             {@link OpenFileOptions#setCacheLocationPolicyClass(String)}.
   */
  @Deprecated
  public OpenFileOptions setLocationPolicyClass(String className) {
    try {
      @SuppressWarnings("unchecked") Class<FileWriteLocationPolicy> clazz =
          (Class<FileWriteLocationPolicy>) Class.forName(className);
      mCacheLocationPolicy =
          CommonUtils.createNewClassInstance(clazz, new Class[] {}, new Object[] {});
      return this;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param className the location policy class to use when storing data to Alluxio
   * @return the updated options object
   */
  public OpenFileOptions setCacheLocationPolicyClass(String className) {
    try {
      @SuppressWarnings("unchecked") Class<FileWriteLocationPolicy> clazz =
          (Class<FileWriteLocationPolicy>) Class.forName(className);
      mCacheLocationPolicy =
          CommonUtils.createNewClassInstance(clazz, new Class[] {}, new Object[] {});
      return this;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param className the location policy class to determine where to read a UFS block
   * @return the updated options object
   */
  public OpenFileOptions setUfsReadLocationPolicyClass(String className) {
    mUfsReadLocationPolicy = BlockLocationPolicy.Factory
        .create(CreateOptions.defaults().setLocationPolicyClassName(className));
    return this;
  }

  /**
   * @param readType the {@link ReadType} for this operation
   * @return the updated options object
   */
  public OpenFileOptions setReadType(ReadType readType) {
    mReadType = readType;
    return this;
  }

  /**
   * @param maxUfsReadConcurrency the maximum UFS read concurrency
   * @return the updated options object
   */
  public OpenFileOptions setMaxUfsReadConcurrency(int maxUfsReadConcurrency) {
    mMaxUfsReadConcurrency = maxUfsReadConcurrency;
    return this;
  }

  /**
   * @return the {@link InStreamOptions} representation of this object
   */
  public InStreamOptions toInStreamOptions() {
    return InStreamOptions.defaults().setReadType(mReadType).setLocationPolicy(mCacheLocationPolicy)
        .setMaxUfsReadConcurrency(mMaxUfsReadConcurrency)
        .setUfsReadLocationPolicy(mUfsReadLocationPolicy);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OpenFileOptions)) {
      return false;
    }
    OpenFileOptions that = (OpenFileOptions) o;
    return Objects.equal(mCacheLocationPolicy, that.mCacheLocationPolicy)
        && Objects.equal(mReadType, that.mReadType)
        && Objects.equal(mMaxUfsReadConcurrency, that.mMaxUfsReadConcurrency)
        && Objects.equal(mUfsReadLocationPolicy, that.mUfsReadLocationPolicy);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCacheLocationPolicy, mReadType, mMaxUfsReadConcurrency);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("cacheLocationPolicy", mCacheLocationPolicy)
        .add("maxUfsReadConcurrency", mMaxUfsReadConcurrency)
        .add("readType", mReadType)
        .add("ufsReadLocationPolicy", mUfsReadLocationPolicy)
        .toString();
  }
}
