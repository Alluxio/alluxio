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

package alluxio.client.block.policy.options;

import alluxio.annotation.PublicApi;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.network.TieredIdentityFactory;
import alluxio.wire.TieredIdentity;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * Method options for creating a {@link alluxio.client.block.policy.BlockLocationPolicy}.
 */
@PublicApi
public final class CreateOptions {
  private AlluxioConfiguration mAlluxioConf;
  private String mLocationPolicyClassName;
  private int mDeterministicHashPolicyNumShards;
  private TieredIdentity mTieredIdentity;
  private long mBlockCapacityReserved;
  private String mSpecificWorker;

  /**
   * Creates a new instance of options for {@link alluxio.client.block.policy.BlockLocationPolicy}.
   *
   * Note that the setter for configuration is explicity excluded from this class. To override
   * values in the options after calling {@code defaults()} programmers should use the respective
   * setter.
   *
   * @param alluxioConf Alluxio configuration
   * @return the default {@link CreateOptions}
   */
  public static CreateOptions defaults(AlluxioConfiguration alluxioConf) {
    return new CreateOptions(alluxioConf);
  }

  /**
   * Creates a new instance with defaults.
   *
   * @param alluxioConf Alluxio configuration
   */
  private CreateOptions(AlluxioConfiguration alluxioConf) {
    mAlluxioConf = alluxioConf;
    mLocationPolicyClassName = alluxioConf.get(PropertyKey.USER_BLOCK_WRITE_LOCATION_POLICY);
    mTieredIdentity = TieredIdentityFactory.localIdentity(alluxioConf);
    mDeterministicHashPolicyNumShards = alluxioConf
        .getInt(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS);
    mBlockCapacityReserved = alluxioConf
        .getBytes(alluxio.conf.PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES);
    mSpecificWorker = alluxioConf.getOrDefault(PropertyKey.WORKER_HOSTNAME, null);
  }

  /**
   * @return the configuration used if the policy is set to
   *         {@link alluxio.client.block.policy.LocalFirstPolicy} or
   *         {@link alluxio.client.block.policy.LocalFirstAvoidEvictionPolicy}
   */
  public AlluxioConfiguration getConfiguration() {
    return mAlluxioConf;
  }

  /**
   * @return the reserve capacity bytes if the policy is set to
   *         {@link alluxio.client.block.policy.LocalFirstAvoidEvictionPolicy}
   */
  public long getBlockReservedCapacity() {
    return mBlockCapacityReserved;
  }

  /**
   * @return the location policy class name
   */
  public String getLocationPolicyClassName() {
    return mLocationPolicyClassName;
  }

  /**
   * @return the number of shards to use if the policy is
   *         {@link alluxio.client.block.policy.DeterministicHashPolicy}.
   */
  public int getDeterministicHashPolicyNumShards() {
    return mDeterministicHashPolicyNumShards;
  }

  /**
   * @return the worker hostname for {@link alluxio.client.block.policy.SpecificHostPolicy}
   */
  public String getSpecificWorker() {
    return mSpecificWorker;
  }

  /**
   * @return the tiered identity if the policy is
   *         {@link alluxio.client.block.policy.LocalFirstPolicy} or
   *         {@link alluxio.client.block.policy.LocalFirstAvoidEvictionPolicy}
   */
  public TieredIdentity getTieredIdentity() {
    return mTieredIdentity;
  }

  /**
   * @param blockReservedCapacity the reserve capacity bytes if the policy is set to
   *        {@link alluxio.client.block.policy.LocalFirstAvoidEvictionPolicy}
   * @return the updated options object
   */
  public CreateOptions setBlockReservedCapacity(long blockReservedCapacity) {
    mBlockCapacityReserved = blockReservedCapacity;
    return this;
  }

  /**
   * @param name the location policy class name
   * @return the updated options object
   */
  public CreateOptions setLocationPolicyClassName(String name) {
    mLocationPolicyClassName = name;
    return this;
  }

  /**
   * @param shards the number of shards to use if the policy is
   *        {@link alluxio.client.block.policy.DeterministicHashPolicy}.
   * @return the updated options object
   */
  public CreateOptions setDeterministicHashPolicyNumShards(int shards) {
    mDeterministicHashPolicyNumShards = shards;
    return this;
  }

  /**
   * @param workerHostname the worker hostname to always return for
   *                       {@link alluxio.client.block.policy.SpecificHostPolicy}
   * @return the update options object worker hostname for
   *         {@link alluxio.client.block.policy.SpecificHostPolicy}
   */
  public CreateOptions setSpecificWorker(String workerHostname) {
    mSpecificWorker = workerHostname;
    return this;
  }

  /**
   * @param identity identity to be used if policy is
   *        {@link alluxio.client.block.policy.LocalFirstPolicy} or
   *        {@link alluxio.client.block.policy.LocalFirstAvoidEvictionPolicy}
   * @return the updated options object
   */
  public CreateOptions setTieredIdentity(TieredIdentity identity) {
    mTieredIdentity = identity;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateOptions)) {
      return false;
    }
    CreateOptions that = (CreateOptions) o;
    return Objects.equal(mAlluxioConf, that.mAlluxioConf)
        && Objects.equal(mBlockCapacityReserved, that.mBlockCapacityReserved)
        && Objects.equal(mDeterministicHashPolicyNumShards, that.mDeterministicHashPolicyNumShards)
        && Objects.equal(mLocationPolicyClassName, that.mLocationPolicyClassName)
        && Objects.equal(mSpecificWorker, that.mSpecificWorker)
        && Objects.equal(mTieredIdentity, that.mTieredIdentity);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        mAlluxioConf,
        mBlockCapacityReserved,
        mDeterministicHashPolicyNumShards,
        mLocationPolicyClassName,
        mSpecificWorker,
        mTieredIdentity
    );
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("alluxioConf", mAlluxioConf)
        .add("blockCapcityReserved", mBlockCapacityReserved)
        .add("deterministicHashPolicyNumShards", mDeterministicHashPolicyNumShards)
        .add("locationPolicyClassName", mLocationPolicyClassName)
        .add("specificWorker", mSpecificWorker)
        .add("tieredIdentity", mTieredIdentity)
        .toString();
  }
}
