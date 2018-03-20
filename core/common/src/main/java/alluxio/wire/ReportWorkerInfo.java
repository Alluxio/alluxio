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
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The report worker information.
 */
@NotThreadSafe
public final class ReportWorkerInfo implements Serializable {
  private static final long serialVersionUID = 1176094131827228462L;

  private WorkerNetAddress mAddress = new WorkerNetAddress();
  private long mCapacityBytes;
  private Map<String, Long> mCapacityBytesOnTiers;
  private long mId;
  private int mLastContactSec;
  private long mStartTimeMs;
  private String mState = "";
  private long mUsedBytes;
  private Map<String, Long> mUsedBytesOnTiers;

  /**
   * Creates a new instance of {@link ReportWorkerInfo}.
   */
  public ReportWorkerInfo() {}

  /**
   * Creates a new instance of {@link ReportWorkerInfo} from a thrift representation.
   *
   * @param reportWorkerInfo the thrift representation of a report worker information
   */
  protected ReportWorkerInfo(alluxio.thrift.ReportWorkerInfo reportWorkerInfo) {
    mAddress = new WorkerNetAddress(reportWorkerInfo.getAddress());
    mCapacityBytes = reportWorkerInfo.getCapacityBytes();
    mCapacityBytesOnTiers = reportWorkerInfo.getCapacityBytesOnTiers();
    mId = reportWorkerInfo.getId();
    mLastContactSec = reportWorkerInfo.getLastContactSec();
    mStartTimeMs = reportWorkerInfo.getStartTimeMs();
    mState = reportWorkerInfo.getState();
    mUsedBytes = reportWorkerInfo.getUsedBytes();
    mUsedBytesOnTiers = reportWorkerInfo.getUsedBytesOnTiers();
  }

  /**
   * @return the worker address
   */
  public WorkerNetAddress getAddress() {
    return mAddress;
  }

  /**
   * @return the worker total capacity (in bytes)
   */
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * @return the worker total capacity (in bytes) on tiers
   */
  public Map<String, Long> getCapacityBytesOnTiers() {
    return mCapacityBytesOnTiers;
  }

  /**
   * @return the worker id
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the worker last contact (in seconds)
   */
  public int getLastContactSec() {
    return mLastContactSec;
  }

  /**
   * @return the worker start time (in milliseconds)
   */
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  /**
   * @return the worker state
   */
  public String getState() {
    return mState;
  }

  /**
   * @return the worker used capacity (in bytes)
   */
  public long getUsedBytes() {
    return mUsedBytes;
  }

  /**
   * @return the worker used capacity (in bytes) on tiers
   */
  public Map<String, Long> getUsedBytesOnTiers() {
    return mUsedBytesOnTiers;
  }

  /**
   * @param address the worker address to use
   * @return the report worker information
   */
  public ReportWorkerInfo setAddress(WorkerNetAddress address) {
    Preconditions.checkNotNull(address, "address");
    mAddress = address;
    return this;
  }

  /**
   * @param capacityBytes the worker total capacity (in bytes) to use
   * @return the report worker information
   */
  public ReportWorkerInfo setCapacityBytes(long capacityBytes) {
    mCapacityBytes = capacityBytes;
    return this;
  }

  /**
   * @param capacityBytesOnTiers the total worker capacity (in bytes) to use
   * @return the report worker information
   */
  public ReportWorkerInfo setCapacityBytesOnTiers(Map<String, Long> capacityBytesOnTiers) {
    mCapacityBytesOnTiers = capacityBytesOnTiers;
    return this;
  }

  /**
   * @param id the worker id to use
   * @return the report worker information
   */
  public ReportWorkerInfo setId(long id) {
    mId = id;
    return this;
  }

  /**
   * @param lastContactSec the worker last contact (in seconds) to use
   * @return the report worker information
   */
  public ReportWorkerInfo setLastContactSec(int lastContactSec) {
    mLastContactSec = lastContactSec;
    return this;
  }

  /**
   * @param startTimeMs the worker start time (in milliseconds) to use
   * @return the report worker information
   */
  public ReportWorkerInfo setStartTimeMs(long startTimeMs) {
    mStartTimeMs = startTimeMs;
    return this;
  }

  /**
   * @param state the worker state to use
   * @return the report worker information
   */
  public ReportWorkerInfo setState(String state) {
    Preconditions.checkNotNull(state, "state");
    mState = state;
    return this;
  }

  /**
   * @param usedBytes the worker used capacity (in bytes) to use
   * @return the report worker information
   */
  public ReportWorkerInfo setUsedBytes(long usedBytes) {
    mUsedBytes = usedBytes;
    return this;
  }

  /**
   * @param usedBytesOnTiers the used worker capacity (in bytes) to use
   * @return the report worker information
   */
  public ReportWorkerInfo setUsedBytesOnTiers(Map<String, Long> usedBytesOnTiers) {
    mUsedBytesOnTiers = usedBytesOnTiers;
    return this;
  }

  /**
   * @return thrift representation of the worker information
   */
  protected alluxio.thrift.ReportWorkerInfo toThrift() {
    return new alluxio.thrift.ReportWorkerInfo(mAddress.toThrift(), mCapacityBytes,
        mCapacityBytesOnTiers, mId, mLastContactSec, mStartTimeMs, mState, mUsedBytes,
        mUsedBytesOnTiers);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ReportWorkerInfo)) {
      return false;
    }
    ReportWorkerInfo that = (ReportWorkerInfo) o;
    return mAddress.equals(that.mAddress) && mCapacityBytes == that.mCapacityBytes
        && mCapacityBytesOnTiers.equals(that.mCapacityBytesOnTiers)
        && mId == that.mId && mLastContactSec == that.mLastContactSec
        && mStartTimeMs == that.mStartTimeMs && mState.equals(that.mState)
        && mUsedBytes == that.mUsedBytes
        && mUsedBytesOnTiers.equals(that.mUsedBytesOnTiers);
  }

  /**
   * Determine order from most recently contacted to least recently contacted.
   */
  public static final class LastContactSecComparator implements Comparator<ReportWorkerInfo> {
    @Override
    public int compare(ReportWorkerInfo o1, ReportWorkerInfo o2) {
      return o1.getLastContactSec() - o2.getLastContactSec();
    }

    /**
     * LastContactSecComparator constructor.
     */
    public LastContactSecComparator() {}
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mAddress, mCapacityBytes, mCapacityBytesOnTiers, mId,
        mLastContactSec, mStartTimeMs, mState, mUsedBytes, mUsedBytesOnTiers);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("address", mAddress)
        .add("capacityBytes", mCapacityBytes)
        .add("capacityBytesOnTiers", mCapacityBytesOnTiers)
        .add("id", mId)
        .add("lastContactSec", mLastContactSec)
        .add("startTimeMs", mStartTimeMs)
        .add("state", mState)
        .add("usedBytes", mUsedBytes)
        .add("usedBytesOnTiers", mUsedBytesOnTiers).toString();
  }
}
