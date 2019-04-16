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

package alluxio.util.webui;

/**
 * A wrapper class of the usage info per tier for displaying in the UI.
 * This is mainly used to avoid using Map in jsp, which could cause problem with Java 8.
 * See https://alluxio.atlassian.net/browse/ALLUXIO-22.
 */
public class UIUsageOnTier {
  private final String mTierAlias;
  private final long mCapacityBytes;
  private final long mUsedBytes;

  /**
   * Creates a new instance of {@link UIUsageOnTier}.
   *
   * @param tierAlias tier alias
   * @param capacityBytes capacity in bytes
   * @param usedBytes used space in bytes
   */
  public UIUsageOnTier(String tierAlias, long capacityBytes, long usedBytes) {
    mTierAlias = tierAlias;
    mCapacityBytes = capacityBytes;
    mUsedBytes = usedBytes;
  }

  /**
   * Gets tier alias.
   *
   * @return the tier alias
   */
  public String getTierAlias() {
    return mTierAlias;
  }

  /**
   * Gets capacity bytes.
   *
   * @return capacity in bytes
   */
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * Gets used bytes.
   *
   * @return used space in bytes
   */
  public long getUsedBytes() {
    return mUsedBytes;
  }
}
