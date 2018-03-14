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

  private long mCapacityBytes;
  private Map<String, Long> mCapacityBytesOnTiers;
  private long mFreeBytes;
  private int mLiveWorkerNum;
  private int mLostWorkerNum;
  private long mUsedBytes;
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
    mCapacityBytes = blockMasterInfo.getCapacityBytes();
    mCapacityBytesOnTiers = blockMasterInfo.getCapacityBytesOnTiers();
    mFreeBytes = blockMasterInfo.getFreeBytes();
    mLiveWorkerNum = blockMasterInfo.getLiveWorkerNum();
    mLostWorkerNum = blockMasterInfo.getLostWorkerNum();
    mUsedBytes = blockMasterInfo.getUsedBytes();
    mUsedBytesOnTiers = blockMasterInfo.getUsedBytesOnTiers();
  }

  /**
   * @return the total capacity bytes
   */
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * @return the total capacity bytes on tiers
   */
  public Map<String, Long> getCapacityBytesOnTiers() {
    return mCapacityBytesOnTiers;
  }

  /**
   * @return the free capacity in bytes
   */
  public long getFreeBytes() {
    return mFreeBytes;
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
   * @return the used capacity bytes
   */
  public long getUsedBytes() {
    return mUsedBytes;
  }

  /**
   * @return the used capacity bytes on tiers
   */
  public Map<String, Long> getUsedBytesOnTiers() {
    return mUsedBytesOnTiers;
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
   * @param capacityBytesOnTiers the total capacity bytes on tiers to use
   * @return the block master information
   */
  public BlockMasterInfo setCapacityBytesOnTiers(Map<String, Long> capacityBytesOnTiers) {
    mCapacityBytesOnTiers = new HashMap<>(capacityBytesOnTiers);
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
   * @param usedBytes the used capacity bytes to use
   * @return the block master information
   */
  public BlockMasterInfo setUsedBytes(long usedBytes) {
    mUsedBytes = usedBytes;
    return this;
  }

  /**
   * @param usedBytesOnTiers the used capacity bytes on tiers to use
   * @return the block master information
   */
  public BlockMasterInfo setUsedBytesOnTiers(Map<String, Long> usedBytesOnTiers) {
    mUsedBytesOnTiers = new HashMap<>(usedBytesOnTiers);
    return this;
  }

  /**
   * @return thrift representation of the block master information
   */
  protected alluxio.thrift.BlockMasterInfo toThrift() {
    return new alluxio.thrift.BlockMasterInfo(mCapacityBytes, mCapacityBytesOnTiers,
        mFreeBytes, mLiveWorkerNum, mLostWorkerNum, mUsedBytes, mUsedBytesOnTiers);
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
    return mCapacityBytes == that.mCapacityBytes
        && mCapacityBytesOnTiers.equals(that.mCapacityBytesOnTiers)
        && mFreeBytes == that.mFreeBytes && mLiveWorkerNum == that.mLiveWorkerNum
        && mLostWorkerNum == that.mLostWorkerNum && mUsedBytes == that.mUsedBytes
        && mUsedBytesOnTiers.equals(that.mUsedBytesOnTiers);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCapacityBytes, mCapacityBytesOnTiers, mFreeBytes,
        mLiveWorkerNum, mLostWorkerNum, mUsedBytes, mUsedBytesOnTiers);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("capacityBytes", mCapacityBytes)
        .add("capacityBytesOnTiers", mCapacityBytesOnTiers)
        .add("freeBytes", mFreeBytes)
        .add("liveWorkerNum", mLiveWorkerNum)
        .add("lostWorkerNum", mLostWorkerNum)
        .add("usedBytes", mUsedBytes)
        .add("usedBytesOnTiers", mUsedBytesOnTiers).toString();
  }

  /**
   * Enum representing the fields of the block master info.
   */
  public static enum BlockMasterInfoField {
    CAPACITY_BYTES,
    CAPACITY_BYTES_ON_TIERS,
    FREE_BYTES,
    LIVE_WORKER_NUM,
    LOST_WORKER_NUM,
    USED_BYTES,
    USED_BYTES_ON_TIERS;

    /**
     * @return the thrift representation of this master info field
     */
    public alluxio.thrift.BlockMasterInfoField toThrift() {
      return alluxio.thrift.BlockMasterInfoField.valueOf(name());
    }

    /**
     * @param field the thrift representation of the master info field to create
     * @return the wire type version of the master info field
     */
    public static BlockMasterInfoField fromThrift(alluxio.thrift.BlockMasterInfoField field) {
      return BlockMasterInfoField.valueOf(field.name());
    }
  }
}
