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

  private int mLiveWorkerNum;
  private int mLostWorkerNum;
  private String mTotalCapacity;
  private String mUsedCapacity;
  private String mFreeCapacity;
  private Map<String, String> mTotalCapacityOnTiers;
  private Map<String, String> mUsedCapacityOnTiers;

  /**
   * Creates a new instance of {@link BlockMasterInfo}.
   */
  public BlockMasterInfo() {}

  /**
   * Creates a new instance of {@link BlockMasterInfo} from a thrift representation.
   *
   * @param blockMasterInfo the thrift representation of a block master information
   */
  protected BlockMasterInfo(alluxio.thrift.BlockMasterInfo blockMasterInfo) {
    mLiveWorkerNum = blockMasterInfo.getLiveWorkerNum();
    mLostWorkerNum = blockMasterInfo.getLostWorkerNum();
    mTotalCapacity = blockMasterInfo.getTotalCapacity();
    mUsedCapacity = blockMasterInfo.getUsedCapacity();
    mFreeCapacity = blockMasterInfo.getFreeCapacity();
    mTotalCapacityOnTiers = blockMasterInfo.getTotalCapacityOnTiers();
    mUsedCapacityOnTiers = blockMasterInfo.getUsedCapacityOnTiers();
  }

  /**
   * @return the live worker number
   */
  public int getLiveWorkerNum() {
    return mLiveWorkerNum;
  }

  /**
   * @return the lost worker number
   */
  public int getLostWorkerNum() {
    return mLostWorkerNum;
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
   * @param liveWorkerNum the live worker number
   * @return the block master information
   */
  public BlockMasterInfo setLiveWorkerNum(int liveWorkerNum) {
    mLiveWorkerNum = liveWorkerNum;
    return this;
  }

  /**
   * @param lostWorkerNum the lost worker number
   * @return the block master information
   */
  public BlockMasterInfo setLostWorkerNum(int lostWorkerNum) {
    mLostWorkerNum = lostWorkerNum;
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
   * @param usedBytesOnTiers the used capacity bytes on tiers to use
   * @return the block master information
   */
  public BlockMasterInfo setUsedCapacityOnTiers(Map<String, Long> usedBytesOnTiers) {
    mUsedCapacityOnTiers = new HashMap<>();
    for (Map.Entry<String, Long> entry : usedBytesOnTiers.entrySet()) {
      long capacity = entry.getValue();
      if (capacity > 0) {
        mUsedCapacityOnTiers.put(entry.getKey(), FormatUtils.getSizeFromBytes(entry.getValue()));
      }
    }
    return this;
  }

  /**
   * @return thrift representation of the block master information
   */
  protected alluxio.thrift.BlockMasterInfo toThrift() {
    return new alluxio.thrift.BlockMasterInfo(mLiveWorkerNum, mLostWorkerNum, mTotalCapacity,
        mUsedCapacity, mFreeCapacity, mTotalCapacityOnTiers, mUsedCapacityOnTiers);
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
    return mLiveWorkerNum == that.mLiveWorkerNum && mLostWorkerNum == that.mLostWorkerNum
        && mTotalCapacity.equals(that.mTotalCapacity) && mUsedCapacity.equals(that.mUsedCapacity)
        && mFreeCapacity.equals(that.mFreeCapacity)
        && mTotalCapacityOnTiers.equals(that.mTotalCapacityOnTiers)
        && mUsedCapacityOnTiers.equals(that.mUsedCapacityOnTiers);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mLiveWorkerNum, mLostWorkerNum, mTotalCapacity, mUsedCapacity,
        mFreeCapacity, mTotalCapacityOnTiers, mUsedCapacityOnTiers);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("liveWorkerNum", mLiveWorkerNum)
        .add("lostWorkerNum", mLostWorkerNum).add("totalCapacity", mTotalCapacity)
        .add("usedCapacity", mUsedCapacity).add("freeCapacity", mFreeCapacity)
        .add("totalCapacityOnTiers", mTotalCapacityOnTiers)
        .add("usedCapacityOnTiers", mUsedCapacityOnTiers).toString();
  }
}
