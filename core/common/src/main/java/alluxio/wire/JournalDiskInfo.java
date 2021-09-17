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

package alluxio.wire;

import com.google.common.base.MoreObjects;

import java.io.Serializable;

/**
 * A class representing the state of a physical device.
 */
public class JournalDiskInfo implements Serializable {
  private static final long serialVersionUID = 12938071234234123L;

  private final String mDiskPath;
  private final long mUsedBytes;
  private final long mTotalAllocatedBytes;
  private final long mAvailableBytes;
  private final double mPercentAvailable;
  private final String mMountPath;

  /**
   * Create a new instance of {@link JournalDiskInfo} representing the current utilization for a
   * particular block device.
   *
   * @param diskPath            the path to the raw device
   * @param totalAllocatedBytes the total filesystem
   * @param usedBytes           the amount of bytes used by the filesystem
   * @param availableBytes      the amount of bytes available on the filesystem
   * @param mountPath           the path where the device is mounted
   */
  public JournalDiskInfo(String diskPath, long totalAllocatedBytes, long usedBytes,
      long availableBytes, String mountPath) {
    mDiskPath = diskPath;
    mTotalAllocatedBytes = totalAllocatedBytes * 1024;
    mUsedBytes = usedBytes * 1024;
    mAvailableBytes = availableBytes * 1024;
    mMountPath = mountPath;
    mPercentAvailable = 100 * ((double) mAvailableBytes / (mAvailableBytes + mUsedBytes));
  }

  /**
   * @return the raw device path
   */
  public String getDiskPath() {
    return mDiskPath;
  }

  /**
   * @return the bytes used by the device
   */
  public long getUsedBytes() {
    return mUsedBytes;
  }

  /**
   * @return the total bytes allocated for the filesystem on this device
   */
  public long getTotalAllocatedBytes() {
    return mTotalAllocatedBytes;
  }

  /**
   * @return the remaining available bytes on this disk
   */
  public long getAvailableBytes() {
    return mAvailableBytes;
  }

  /**
   * @return the path where this disk is mounted
   */
  public String getMountPath() {
    return mMountPath;
  }

  /**
   * @return the percent of remaining space available on this disk
   */
  public double getPercentAvailable() {
    return mPercentAvailable;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("diskPath", mDiskPath)
        .add("totalAllocatedBytes", mTotalAllocatedBytes).add("usedBytes", mUsedBytes)
        .add("availableBytes", mAvailableBytes).add("percentAvailable", mPercentAvailable)
        .add("mountPath", mMountPath).toString();
  }
}
