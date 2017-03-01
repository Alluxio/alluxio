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

package alluxio.client.file.policy.options;

import alluxio.annotation.PublicApi;

import com.google.common.base.Objects;

/**
 * Method options for creating a {@link BlockLocationPolicyCreateOptions}.
 */
@PublicApi
public final class BlockLocationPolicyCreateOptions {
  private String mLocationPolicyClassName;
  private int mDeterministicHashPolicyNumShards;

  /**
   * @return the default {@link BlockLocationPolicyCreateOptions}
   */
  public static BlockLocationPolicyCreateOptions defaults() {
    return new BlockLocationPolicyCreateOptions();
  }

  /**
   * Creates a new instance with defaults.
   */
  private BlockLocationPolicyCreateOptions() {
    mDeterministicHashPolicyNumShards = 1;
  }

  /**
   * @return the location policy class name
   */
  public String getLocationPolicyClassName() {
    return mLocationPolicyClassName;
  }

  /**
   * @return the number of shards to use if the policy is
   *         {@link alluxio.client.file.policy.DeterministicHashPolicy}.
   */
  public int getDeterministicHashPolicyNumShards() {
    return mDeterministicHashPolicyNumShards;
  }

  /**
   * @param name the location policy class name
   * @return the updated options object
   */
  public BlockLocationPolicyCreateOptions setLocationPolicyClassName(String name) {
    mLocationPolicyClassName = name;
    return this;
  }

  /**
   * @param shards the number of shards to use if the policy is
   *        {@link alluxio.client.file.policy.DeterministicHashPolicy}.
   * @return the updated options object
   */
  public BlockLocationPolicyCreateOptions setDeterministicHashPolicyNumShards(int shards) {
    mDeterministicHashPolicyNumShards = shards;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BlockLocationPolicyCreateOptions)) {
      return false;
    }
    BlockLocationPolicyCreateOptions that = (BlockLocationPolicyCreateOptions) o;
    return Objects.equal(mLocationPolicyClassName, that.mLocationPolicyClassName)
        && Objects.equal(mDeterministicHashPolicyNumShards, that.mDeterministicHashPolicyNumShards);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mLocationPolicyClassName, mDeterministicHashPolicyNumShards);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("locationPolicyClassName", mLocationPolicyClassName)
        .add("deterministicHashPolicyNumShards", mDeterministicHashPolicyNumShards)
        .toString();
  }
}
