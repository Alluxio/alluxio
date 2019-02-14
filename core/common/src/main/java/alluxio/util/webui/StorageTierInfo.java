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

import alluxio.util.FormatUtils;

/**
 * Class to make referencing tiered storage information more intuitive.
 */
public final class StorageTierInfo {
  private final String mStorageTierAlias;
  private final long mCapacityBytes;
  private final long mUsedBytes;
  private final int mUsedPercent;
  private final long mFreeBytes;
  private final int mFreePercent;

  /**
   * Instantiates a new Storage tier info.
   *
   * @param storageTierAlias the storage tier alias
   * @param capacityBytes the capacity bytes
   * @param usedBytes the used bytes
   */
  public StorageTierInfo(String storageTierAlias, long capacityBytes, long usedBytes) {
    mStorageTierAlias = storageTierAlias;
    mCapacityBytes = capacityBytes;
    mUsedBytes = usedBytes;
    mFreeBytes = mCapacityBytes - mUsedBytes;
    mUsedPercent = (int) (100L * mUsedBytes / mCapacityBytes);
    mFreePercent = 100 - mUsedPercent;
  }

  /**
   * Gets storage tier alias.
   *
   * @return the storage alias
   */
  public String getStorageTierAlias() {
    return mStorageTierAlias;
  }

  /**
   * Gets capacity.
   *
   * @return the capacity
   */
  public String getCapacity() {
    return FormatUtils.getSizeFromBytes(mCapacityBytes);
  }

  /**
   * Gets free capacity.
   *
   * @return the free capacity
   */
  public String getFreeCapacity() {
    return FormatUtils.getSizeFromBytes(mFreeBytes);
  }

  /**
   * Gets free space percent.
   *
   * @return the free space as a percentage
   */
  public int getFreeSpacePercent() {
    return mFreePercent;
  }

  /**
   * Gets used capacity.
   *
   * @return the used capacity
   */
  public String getUsedCapacity() {
    return FormatUtils.getSizeFromBytes(mUsedBytes);
  }

  /**
   * Gets used space percent.
   *
   * @return the used space as a percentage
   */
  public int getUsedSpacePercent() {
    return mUsedPercent;
  }
}
