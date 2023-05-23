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

import alluxio.annotation.PublicApi;
import alluxio.heartbeat.HeartbeatThread;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.io.Serializable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The heartbeat thread information.
 */
@NotThreadSafe
@PublicApi
public class HeartbeatThreadInfo implements Serializable {
  private static final long serialVersionUID = -5457846691141843682L;

  private String mThreadName = "";
  private long mCount;
  private HeartbeatThread.Status mStatus;
  private String mPreviousReport = "";
  private String mStartHeartbeatTime = "";
  private String mStartTickTime = "";

  /**
   * Creates a new instance of {@link HeartbeatThreadInfo}.
   */
  public HeartbeatThreadInfo() {}

  /**
   * @return the thread name
   */
  public String getThreadName() {
    return mThreadName;
  }

  /**
   * @param threadName set thread name
   * @return the heartbeat thread information
   */
  public HeartbeatThreadInfo setThreadName(String threadName) {
    mThreadName = threadName;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HeartbeatThreadInfo)) {
      return false;
    }
    HeartbeatThreadInfo that = (HeartbeatThreadInfo) o;
    return mThreadName.equals(that.mThreadName)
        && mPreviousReport.equals(that.mPreviousReport)
        && mCount == that.mCount
        && mStatus == that.mStatus
        && mStartHeartbeatTime.equals(that.mStartHeartbeatTime)
        && mStartTickTime.equals(that.mStartTickTime);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mThreadName, mCount, mStatus, mStartTickTime,
        mStartHeartbeatTime, mPreviousReport);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("threadName", mThreadName)
        .add("count", mCount)
        .add("status", mStatus)
        .add("startHeartbeatTime", mStartHeartbeatTime)
        .add("startTickTime", mStartTickTime)
        .add("previousReport", mPreviousReport)
        .toString();
  }

  /**
   * @param count the current heartbeat count
   * @return the heartbeat thread information
   */
  public HeartbeatThreadInfo setCount(long count) {
    mCount = count;
    return this;
  }

  /**
   * @param status the current heartbeat status
   * @return the heartbeat thread information
   */
  public HeartbeatThreadInfo setStatus(HeartbeatThread.Status status) {
    mStatus = status;
    return this;
  }

  /**
   * @param previousReport the previous heartbeat report
   * @return the heartbeat thread information
   */
  public HeartbeatThreadInfo setPreviousReport(String previousReport) {
    mPreviousReport = previousReport;
    return this;
  }

  /**
   * @return the current heartbeat invoke count
   */
  public long getCount() {
    return mCount;
  }

  /**
   * @return the heartbeat status
   */
  public HeartbeatThread.Status getStatus() {
    return mStatus;
  }

  /**
   * @return the report for previous heartbeat
   */
  public String getPreviousReport() {
    return mPreviousReport;
  }

  /**
   * @param startHeartbeatTime the current heartbeat thread heartbeat time
   * @return the heartbeat thread information
   */
  public HeartbeatThreadInfo setStartHeartbeatTime(String startHeartbeatTime) {
    mStartHeartbeatTime = startHeartbeatTime;
    return this;
  }

  /**
   * @return the current heartbeat thread heartbeat time
   */
  public String getStartHeartbeatTime() {
    return mStartHeartbeatTime;
  }

  /**
   * @param startTickTime the current heartbeat thread tick time
   * @return the heartbeat thread information
   */
  public HeartbeatThreadInfo setStartTickTime(String startTickTime) {
    mStartTickTime = startTickTime;
    return this;
  }

  /**
   * @return the current heartbeat thread tick time
   */
  public String getStartTickTime() {
    return mStartTickTime;
  }
}
