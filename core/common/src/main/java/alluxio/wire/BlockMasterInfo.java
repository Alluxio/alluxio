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

import alluxio.util.FormatUtils;

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The Alluxio block master information.
 */
@NotThreadSafe
public final class BlockMasterInfo implements Serializable {
  private static final long serialVersionUID = -7436215337909224255L;

  private long mWorkerNum;
  private String mTotalCapacity;
  private String mUsedCapacity;
  private String mFreeCapacity;
  private Map<String, String>  mTotalCapacityOnTiers;
  private Map<String, String> mUsedCapacityOnTiers;

  /**
   * Creates a new instance of {@link BlockMasterInfo}.
   */
  public BlockMasterInfo() {}

  /**
   * Creates a new instance of {@link BlockMasterInfo} from a thrift representation.
   *
   * @param blockMasterInfo the thrift representation of a worker information
   */
  protected BlockMasterInfo(alluxio.thrift.BlockMasterInfo blockMasterInfo) {
    mWorkerNum = blockMasterInfo.getWorkerNum();
    mTotalCapacity = blockMasterInfo.getTotalCapacity();
    mUsedCapacity = blockMasterInfo.getUsedCapacity();
    mFreeCapacity = blockMasterInfo.getFreeCapacity();
    mTotalCapacityOnTiers = blockMasterInfo.getTotalCapacityOnTiers();
    mUsedCapacityOnTiers = blockMasterInfo.getUsedCapacityOnTiers();
  }

  /**
   * @return the worker number
   */
  public long getWorkerNum() {
    return mWorkerNum;
  }

  /**
   * @return the total capacity
   */
  public String getTotalCapacity() {
    return mTotalCapacity;
  }

  /**
   * @return the used capacity
   */
  public String getUsedCapacity() {
    return mUsedCapacity;
  }

  /**
   * @return the free capacity
   */
  public String getFreeCapacity() {
    return mFreeCapacity;
  }

  /**
   * @return the total capacity on tiers
   */
  public Map<String, String> getTotalCapacityOnTiers() {
    return mTotalCapacityOnTiers;
  }

  /**
   * @return the used capacity on tiers
   */
  public Map<String, String> getUsedCapacityOnTiers() {
    return mUsedCapacityOnTiers;
  }

  /**
   * @param workerNum the workerNum to use
   * @return the block master information
   */
  public BlockMasterInfo setWorkerNum(long workerNum) {
    mWorkerNum = workerNum;
    return this;
  }

  /**
   * @param totalCapacity the total capacity to use
   * @return the block master information
   */
  public BlockMasterInfo setTotalCapacity(long totalCapacity) {
    mTotalCapacity = FormatUtils.getSizeFromBytes(totalCapacity);
    return this;
  }

  /**
   * @param usedCapacity the used capacity to use
   * @return the block master information
   */
  public BlockMasterInfo setUsedCapacity(long usedCapacity) {
    mUsedCapacity = FormatUtils.getSizeFromBytes(usedCapacity);
    return this;
  }

  /**
   * @param freeCapacity the free capacity to use
   * @return the block master information
   */
  public BlockMasterInfo setFreeCapacity(long freeCapacity) {
    mFreeCapacity = FormatUtils.getSizeFromBytes(freeCapacity);
    return this;
  }

  /**
   * @param totalBytesOnTiers the total capacity bytes on tiers to use
   * @return the block master information
   */
  public BlockMasterInfo setTotalCapcityOnTiers(Map<String, Long> totalBytesOnTiers) {
    mTotalCapacityOnTiers = new HashMap<>();
    for (Map.Entry<String, Long> entry : totalBytesOnTiers.entrySet()) {
      long capacity = entry.getValue();
      if (capacity > 0) {
        mTotalCapacityOnTiers.put(entry.getKey(), FormatUtils.getSizeFromBytes(entry.getValue()));
      }
    }
    return this;
  }

  /**
   * @param usedBytesOnTiers the total capacity bytes on tiers to use
   * @return the block master information
   */
  public BlockMasterInfo setUsedCapacityOnTiers(Map<String, Long> usedBytesOnTiers) {
    mUsedCapacityOnTiers = new HashMap<>();
    for (Map.Entry<String, Long> entry : usedBytesOnTiers.entrySet()) {
      long usedBytes = entry.getValue();
      if (usedBytes > 0) {
        mUsedCapacityOnTiers.put(entry.getKey(), FormatUtils.getSizeFromBytes(entry.getValue()));
      }
    }
    return this;
  }

  /**
   * @return thrift representation of the cluster information
   */
  protected alluxio.thrift.BlockMasterInfo toThrift() {
    return new alluxio.thrift.BlockMasterInfo(mWorkerNum, mTotalCapacity, mUsedCapacity,
        mFreeCapacity, mTotalCapacityOnTiers, mUsedCapacityOnTiers);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BlockMasterInfo)) {
      return false;
    }
    BlockMasterInfo that = (BlockMasterInfo) o;
    return mWorkerNum == that.mWorkerNum && mTotalCapacity.equals(that.mTotalCapacity)
        && mUsedCapacity.equals(that.mUsedCapacity) && mFreeCapacity.equals(that.mFreeCapacity)
        && mTotalCapacityOnTiers.equals(that.mTotalCapacityOnTiers)
        && mUsedCapacityOnTiers.equals(that.mUsedCapacityOnTiers);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mWorkerNum, mTotalCapacity, mUsedCapacity, mFreeCapacity,
        mTotalCapacityOnTiers, mUsedCapacityOnTiers);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("workerNum", mWorkerNum)
        .add("totalCapacity", mTotalCapacity).add("usedCapacity", mUsedCapacity)
        .add("freeCapacity", mFreeCapacity).add("totalCapacityOnTiers", mTotalCapacityOnTiers)
        .add("usedCapacityOnTiers", mUsedCapacityOnTiers).toString();
  }
}
