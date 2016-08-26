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

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The worker descriptor.
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

  /**
   * Creates a new instance of {@link WorkerInfo}.
   */
  public WorkerInfo() {}

  /**
   * Creates a new instance of {@link WorkerInfo} from a thrift representation.
   *
   * @param workerInfo the thrift representation of a worker descriptor
   */
  protected WorkerInfo(alluxio.thrift.WorkerInfo workerInfo) {
    mId = workerInfo.getId();
    mAddress = new WorkerNetAddress(workerInfo.getAddress());
    mLastContactSec = workerInfo.getLastContactSec();
    mState = workerInfo.getState();
    mCapacityBytes = workerInfo.getCapacityBytes();
    mUsedBytes = workerInfo.getUsedBytes();
    mStartTimeMs = workerInfo.getStartTimeMs();
  }

  /**
   * @return the worker id
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the worker address
   */
  public WorkerNetAddress getAddress() {
    return mAddress;
  }

  /**
   * @return the worker last contact (in seconds)
   */
  public int getLastContactSec() {
    return mLastContactSec;
  }

  /**
   * @return the worker state
   */
  public String getState() {
    return mState;
  }

  /**
   * @return the worker total capacity (in bytes)
   */
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * @return the worker used capacity (in bytes)
   */
  public long getUsedBytes() {
    return mUsedBytes;
  }

  /**
   * @return the worker start time (in milliseconds)
   */
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  /**
   * @param id the worker id to use
   * @return the worker descriptor
   */
  public WorkerInfo setId(long id) {
    mId = id;
    return this;
  }

  /**
   * @param address the worker address to use
   * @return the worker descriptor
   */
  public WorkerInfo setAddress(WorkerNetAddress address) {
    Preconditions.checkNotNull(address);
    mAddress = address;
    return this;
  }

  /**
   * @param lastContactSec the worker last contact (in seconds) to use
   * @return the worker descriptor
   */
  public WorkerInfo setLastContactSec(int lastContactSec) {
    mLastContactSec = lastContactSec;
    return this;
  }

  /**
   * @param state the worker state to use
   * @return the worker descriptor
   */
  public WorkerInfo setState(String state) {
    Preconditions.checkNotNull(state);
    mState = state;
    return this;
  }

  /**
   * @param capacityBytes the worker total capacity (in bytes) to use
   * @return the worker descriptor
   */
  public WorkerInfo setCapacityBytes(long capacityBytes) {
    mCapacityBytes = capacityBytes;
    return this;
  }

  /**
   * @param usedBytes the worker used capacity (in bytes) to use
   * @return the worker descriptor
   */
  public WorkerInfo setUsedBytes(long usedBytes) {
    mUsedBytes = usedBytes;
    return this;
  }

  /**
   * @param startTimeMs the worker start time (in milliseconds) to use
   * @return the worker descriptor
   */
  public WorkerInfo setStartTimeMs(long startTimeMs) {
    mStartTimeMs = startTimeMs;
    return this;
  }

  /**
   * @return thrift representation of the worker descriptor
   */
  protected alluxio.thrift.WorkerInfo toThrift() {
    return new alluxio.thrift.WorkerInfo(mId, mAddress.toThrift(), mLastContactSec, mState,
        mCapacityBytes, mUsedBytes, mStartTimeMs);
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
    return mId == that.mId && mAddress.equals(that.mAddress)
        && mLastContactSec == that.mLastContactSec && mState.equals(that.mState)
        && mCapacityBytes == that.mCapacityBytes && mUsedBytes == that.mUsedBytes
        && mStartTimeMs == that.mStartTimeMs;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mId, mAddress, mLastContactSec, mState, mCapacityBytes, mUsedBytes,
        mStartTimeMs);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("id", mId).add("address", mAddress)
        .add("lastContactSec", mLastContactSec).add("state", mState)
        .add("capacityBytes", mCapacityBytes).add("usedBytes", mUsedBytes)
        .add("startTimeMs", mStartTimeMs).toString();
  }

}
