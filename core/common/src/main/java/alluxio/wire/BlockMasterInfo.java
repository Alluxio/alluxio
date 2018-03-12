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
  private long mCapacityBytes;
  private long mUsedBytes;
  private long mFreeBytes;
  private Map<String, Long> mCapacityBytesOnTiers;
  private Map<String, Long> mUsedBytesOnTiers;

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
    mCapacityBytes = blockMasterInfo.getCapacityBytes();
    mUsedBytes = blockMasterInfo.getUsedBytes();
    mFreeBytes = blockMasterInfo.getFreeBytes();
    mCapacityBytesOnTiers = blockMasterInfo.getCapacityBytesOnTiers();
    mUsedBytesOnTiers = blockMasterInfo.getUsedBytesOnTiers();
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
   * @return the total capacity bytes
   */
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * @return the used capacity bytes
   */
  public long getUsedBytes() {
    return mUsedBytes;
  }

  /**
   * @return the free capacity in bytes
   */
  public long getFreeBytes() {
    return mFreeBytes;
  }

  /**
   * @return the total capacity bytes on tiers
   */
  public Map<String, Long> getCapacityBytesOnTiers() {
    return mCapacityBytesOnTiers;
  }

  /**
   * @return the used capacity bytes on tiers
   */
  public Map<String, Long> getUsedBytesOnTiers() {
    return mUsedBytesOnTiers;
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
   * @param capacityBytes the total capacity bytes to use
   * @return the block master information
   */
  public BlockMasterInfo setCapacityBytes(long capacityBytes) {
    mCapacityBytes = capacityBytes;
    return this;
  }

  /**
   * @param usedBytes the used capacity bytes to use
   * @return the block master information
   */
  public BlockMasterInfo setUsedBytes(long usedBytes) {
    mUsedBytes = usedBytes;
    return this;
  }

  /**
   * @param freeBytes the free capacity bytes to use
   * @return the block master information
   */
  public BlockMasterInfo setFreeBytes(long freeBytes) {
    mFreeBytes = freeBytes;
    return this;
  }

  /**
   * @param capacityBytesOnTiers the total capacity bytes on tiers to use
   * @return the block master information
   */
  public BlockMasterInfo setCapacityBytesOnTiers(Map<String, Long> capacityBytesOnTiers) {
    mCapacityBytesOnTiers = new HashMap<>();
    for (Map.Entry<String, Long> entry : capacityBytesOnTiers.entrySet()) {
      long capacity = entry.getValue();
      if (capacity > 0) {
        mCapacityBytesOnTiers.put(entry.getKey(), capacity);
      }
    }
    return this;
  }

  /**
   * @param usedBytesOnTiers the used capacity bytes on tiers to use
   * @return the block master information
   */
  public BlockMasterInfo setUsedBytesOnTiers(Map<String, Long> usedBytesOnTiers) {
    mUsedBytesOnTiers = new HashMap<>();
    for (Map.Entry<String, Long> entry : usedBytesOnTiers.entrySet()) {
      long capacity = entry.getValue();
      if (capacity > 0) {
        mUsedBytesOnTiers.put(entry.getKey(), capacity);
      }
    }
    return this;
  }

  /**
   * @return thrift representation of the block master information
   */
  protected alluxio.thrift.BlockMasterInfo toThrift() {
    return new alluxio.thrift.BlockMasterInfo(mLiveWorkerNum, mLostWorkerNum, mCapacityBytes,
        mUsedBytes, mFreeBytes, mCapacityBytesOnTiers, mUsedBytesOnTiers);
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
        && mCapacityBytes == that.mCapacityBytes && mUsedBytes == that.mUsedBytes
        && mFreeBytes == that.mFreeBytes
        && mCapacityBytesOnTiers.equals(that.mCapacityBytesOnTiers)
        && mUsedBytesOnTiers.equals(that.mUsedBytesOnTiers);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mLiveWorkerNum, mLostWorkerNum, mCapacityBytes, mUsedBytes,
        mFreeBytes, mCapacityBytesOnTiers, mUsedBytesOnTiers);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("liveWorkerNum", mLiveWorkerNum)
        .add("lostWorkerNum", mLostWorkerNum).add("capacityBytes", mCapacityBytes)
        .add("usedBytes", mUsedBytes).add("freeBytes", mFreeBytes)
        .add("capacityBytesOnTiers", mCapacityBytesOnTiers)
        .add("usedBytesOnTiers", mUsedBytesOnTiers).toString();
  }
}
