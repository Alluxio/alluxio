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
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The worker information.
 */
@NotThreadSafe
public final class WorkerInfo implements Serializable {
  private static final long serialVersionUID = -454711814438216780L;

  private long mId;
  private WorkerNetAddress mAddress = new WorkerNetAddress();
  private int mLastContactSec;
  private String mState = "";
  private long mCapacityBytes;
  private long mUsedBytes;
  private long mStartTimeMs;
  private Map<String, Long> mCapacityBytesOnTiers;
  private Map<String, Long> mUsedBytesOnTiers;
  private long mBlockCount;

  /**
   * Creates a new instance of {@link WorkerInfo}.
   */
  public WorkerInfo() {}

  /**
   * @return the worker id
   */
  @ApiModelProperty(value = "Worker id, used to identify the worker internally")
  public long getId() {
    return mId;
  }

  /**
   * @return the worker address
   */
  @ApiModelProperty(value = "Address of the worker")
  public WorkerNetAddress getAddress() {
    return mAddress;
  }

  /**
   * @return the worker last contact (in seconds)
   */
  @ApiModelProperty(value = "Seconds since the worker's last contact")
  public int getLastContactSec() {
    return mLastContactSec;
  }

  /**
   * @return the worker state
   */
  @ApiModelProperty(value = "Operation state of the worker", example = "In Service")
  public String getState() {
    return mState;
  }

  /**
   * @return the worker total capacity (in bytes)
   */
  @ApiModelProperty(value = "Size of the worker's local storage in bytes")
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * @return the worker used capacity (in bytes)
   */
  @ApiModelProperty(value = "Number of bytes used of the worker's local storage")
  public long getUsedBytes() {
    return mUsedBytes;
  }

  /**
   * @return the worker start time (in milliseconds)
   */
  @ApiModelProperty(value = "Start time of the worker in epoch time in milliseconds")
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  /**
   * @return the worker total capacity (in bytes) on tiers
   */
  @ApiModelProperty(value = "The max capacity of each of the worker's tiers in bytes")
  public Map<String, Long> getCapacityBytesOnTiers() {
    return mCapacityBytesOnTiers;
  }

  /**
   * @return the worker used capacity (in bytes) on tiers
   */
  @ApiModelProperty(value = "The number of bytes currently used on each of the worker's tiers")
  public Map<String, Long> getUsedBytesOnTiers() {
    return mUsedBytesOnTiers;
  }

  /**
   * @param id the worker id to use
   * @return the worker information
   */
  public WorkerInfo setId(long id) {
    mId = id;
    return this;
  }

  /**
   * @param address the worker address to use
   * @return the worker information
   */
  public WorkerInfo setAddress(WorkerNetAddress address) {
    Preconditions.checkNotNull(address, "address");
    mAddress = address;
    return this;
  }

  /**
   * @param lastContactSec the worker last contact (in seconds) to use
   * @return the worker information
   */
  public WorkerInfo setLastContactSec(int lastContactSec) {
    mLastContactSec = lastContactSec;
    return this;
  }

  /**
   * @param state the worker state to use
   * @return the worker information
   */
  public WorkerInfo setState(String state) {
    Preconditions.checkNotNull(state, "state");
    mState = state;
    return this;
  }

  /**
   * @param capacityBytes the worker total capacity (in bytes) to use
   * @return the worker information
   */
  public WorkerInfo setCapacityBytes(long capacityBytes) {
    mCapacityBytes = capacityBytes;
    return this;
  }

  /**
   * @param usedBytes the worker used capacity (in bytes) to use
   * @return the worker information
   */
  public WorkerInfo setUsedBytes(long usedBytes) {
    mUsedBytes = usedBytes;
    return this;
  }

  /**
   * @param startTimeMs the worker start time (in milliseconds) to use
   * @return the worker information
   */
  public WorkerInfo setStartTimeMs(long startTimeMs) {
    mStartTimeMs = startTimeMs;
    return this;
  }

  /**
   * @param capacityBytesOnTiers the total worker capacity (in bytes) to use
   * @return the worker information
   */
  public WorkerInfo setCapacityBytesOnTiers(Map<String, Long> capacityBytesOnTiers) {
    mCapacityBytesOnTiers = new HashMap<>(capacityBytesOnTiers);
    return this;
  }

  /**
   * @param usedBytesOnTiers the used worker capacity (in bytes) to use
   * @return the worker information
   */
  public WorkerInfo setUsedBytesOnTiers(Map<String, Long> usedBytesOnTiers) {
    mUsedBytesOnTiers = new HashMap<>(usedBytesOnTiers);
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WorkerInfo)) {
      return false;
    }
    WorkerInfo that = (WorkerInfo) o;
    return mId == that.mId && Objects.equal(mAddress, that.mAddress)
        && mLastContactSec == that.mLastContactSec && Objects.equal(mState, that.mState)
        && mCapacityBytes == that.mCapacityBytes && mUsedBytes == that.mUsedBytes
        && mStartTimeMs == that.mStartTimeMs
        && Objects.equal(mCapacityBytesOnTiers, that.mCapacityBytesOnTiers)
        && Objects.equal(mUsedBytesOnTiers, that.mUsedBytesOnTiers);
  }

  /**
   * @param blockCount the worker block count
   * @return the worker information
   */
  public WorkerInfo setBlockCount(long blockCount) {
    mBlockCount = blockCount;
    return this;
  }

  /**
   * @return the worker block count
   */
  @ApiModelProperty(value = "Number of worker block count")
  public long getBlockCount() {
    return mBlockCount;
  }

  /**
   * Determine order from most recently contacted to least recently contacted.
   */
  public static final class LastContactSecComparator implements Comparator<WorkerInfo>,
      Serializable {
    private static final long serialVersionUID = 0L;

    @Override
    public int compare(WorkerInfo o1, WorkerInfo o2) {
      return o1.getLastContactSec() - o2.getLastContactSec();
    }

    /**
     * LastContactSecComparator constructor.
     */
    public LastContactSecComparator() {}
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mId, mAddress, mLastContactSec, mState, mCapacityBytes, mUsedBytes,
        mStartTimeMs, mCapacityBytesOnTiers, mUsedBytesOnTiers);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", mId).add("address", mAddress)
        .add("lastContactSec", mLastContactSec).add("state", mState)
        .add("capacityBytes", mCapacityBytes).add("usedBytes", mUsedBytes)
        .add("startTimeMs", mStartTimeMs).add("capacityBytesOnTiers", mCapacityBytesOnTiers)
        .add("usedBytesOnTiers", mUsedBytesOnTiers).toString();
  }
}
