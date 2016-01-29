/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.wire;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 * The worker descriptor.
 */
public class WorkerInfo {
  @JsonProperty("id")
  private long mId;
  @JsonProperty("address")
  private WorkerNetAddress mAddress;
  @JsonProperty("lastContactSec")
  private int mLastContactSec;
  @JsonProperty("state")
  private String mState;
  @JsonProperty("capacityBytes")
  private long mCapacityBytes;
  @JsonProperty("usedBytes")
  private long mUsedBytes;
  @JsonProperty("startTimeMs")
  private long mStartTimeMs;

  /**
   * Creates a new instance of {@link WorkerInfo}.
   */
  public WorkerInfo() {}

  /**
   * Creates a new instance of {@link WorkerInfo}.
   *
   * @param id the worker id to use
   * @param address the worker address to use
   * @param lastContactSec the worker last contact (in seconds) to use
   * @param state the worker state to use
   * @param capacityBytes the worker total capacity (in bytes) to use
   * @param usedBytes the worker used capacity (in bytes) to use
   * @param startTimeMs the worker start time (in milliseconds) to use
   */
  public WorkerInfo(long id, WorkerNetAddress address, int lastContactSec, String state,
                    long capacityBytes, long usedBytes, long startTimeMs) {
    mId = id;
    mAddress = address;
    mLastContactSec = lastContactSec;
    mState = state;
    mCapacityBytes = capacityBytes;
    mUsedBytes = usedBytes;
    mStartTimeMs = startTimeMs;
  }

  /**
   * Creates a new instance of {@link WorkerInfo} from a thrift representation.
   *
   * @param workerInfo the thrift representation of a worker descriptor
   */
  public WorkerInfo(tachyon.thrift.WorkerInfo workerInfo) {
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
   */
  public void setId(long id) {
    mId = id;
  }

  /**
   * @param address the worker address to use
   */
  public void setAddress(WorkerNetAddress address) {
    mAddress = address;
  }

  /**
   * @param lastContactSec the worker last contact (in seconds) to use
   */
  public void setLastContactSec(int lastContactSec) {
    mLastContactSec = lastContactSec;
  }

  /**
   * @param state the worker state to use
   */
  public void setState(String state) {
    mState = state;
  }

  /**
   * @param capacityBytes the worker total capacity (in bytes) to use
   */
  public void setCapacityBytes(long capacityBytes) {
    mCapacityBytes = capacityBytes;
  }

  /**
   * @param usedBytes the worker used capacity (in bytes) to use
   */
  public void setUsedBytes(long usedBytes) {
    mUsedBytes = usedBytes;
  }

  /**
   * @param startTimeMs the worker start time (in milliseconds) to use
   */
  public void setStartTimeMs(long startTimeMs) {
    mStartTimeMs = startTimeMs;
  }

  /**
   * @return thrift representation of the block descriptor
   */
  public tachyon.thrift.WorkerInfo toThrift() {
    return new tachyon.thrift.WorkerInfo(mId, mAddress.toThrift(), mLastContactSec, mState,
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
    return Objects.toStringHelper(this).add("id", mId).add("address", mAddress.toString())
        .add("lastContactSec", mLastContactSec).add("state", mState)
        .add("capacityBytes", mCapacityBytes).add("usedBytes", mUsedBytes)
        .add("startTimeMs", mStartTimeMs).toString();
  }

}
