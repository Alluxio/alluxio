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

package alluxio.util.webui;

import alluxio.util.FormatUtils;
import alluxio.wire.WorkerInfo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 * Class to make referencing worker nodes more intuitive. Mainly to avoid implicit association by
 * array indexes.
 */
public final class NodeInfo implements Comparable<NodeInfo> {
  private final String mHost;
  private final int mWebPort;
  private final String mLastContactSec;
  private final String mWorkerState;
  private final String mCapacity;
  private final String mUsedMemory;
  private final int mFreePercent;
  private final int mUsedPercent;
  private final String mUptimeClockTime;
  private final long mWorkerId;

  /**
   * Instantiates a new Node info.
   *
   * @param workerInfo the worker info
   */
  public NodeInfo(WorkerInfo workerInfo) {
    mHost = workerInfo.getAddress().getHost();
    mWebPort = workerInfo.getAddress().getWebPort();
    mLastContactSec = Integer.toString(workerInfo.getLastContactSec());
    mWorkerState = workerInfo.getState();
    mCapacity = FormatUtils.getSizeFromBytes(workerInfo.getCapacityBytes());
    mUsedMemory = FormatUtils.getSizeFromBytes(workerInfo.getUsedBytes());
    if (workerInfo.getCapacityBytes() != 0) {
      mUsedPercent = (int) (100L * workerInfo.getUsedBytes() / workerInfo.getCapacityBytes());
    } else {
      mUsedPercent = 0;
    }
    mFreePercent = 100 - mUsedPercent;
    mUptimeClockTime = WebUtils
        .convertMsToShortClockTime(System.currentTimeMillis() - workerInfo.getStartTimeMs());
    mWorkerId = workerInfo.getId();
  }

  /**
   * Instantiates a new Node info.
   *
   * @param host the host
   * @param webPort the web port
   * @param lastContactSec the last contact sec
   * @param workerState the worker state
   * @param capacity the capacity
   * @param capacityBytes the capacity bytes
   * @param usedBytes the used bytes
   * @param usedMemory the used memory
   * @param freePercent the free percent
   * @param usedPercent the used percent
   * @param uptimeClockTime the uptime clock time
   * @param workerId the worker id
   */
  @JsonCreator
  public NodeInfo(@JsonProperty("host") String host, @JsonProperty("webPort") int webPort,
      @JsonProperty("lastHeartbeat") String lastContactSec,
      @JsonProperty("state") String workerState, @JsonProperty("capacity") String capacity,
      @JsonProperty("capacityBytes") long capacityBytes, @JsonProperty("usedBytes") long usedBytes,
      @JsonProperty("usedMemory") String usedMemory,
      @JsonProperty("freeSpacePercent") int freePercent,
      @JsonProperty("usedSpacePercent") int usedPercent,
      @JsonProperty("uptimeClockTime") String uptimeClockTime,
      @JsonProperty("workerId") long workerId) {
    mHost = host;
    mWebPort = webPort;
    mLastContactSec = lastContactSec;
    mWorkerState = workerState;
    mCapacity = FormatUtils.getSizeFromBytes(capacityBytes);
    mUsedMemory = usedMemory;
    mFreePercent = freePercent;
    mUsedPercent = usedPercent;
    mUptimeClockTime = uptimeClockTime;
    mWorkerId = workerId;
  }

  /**
   * @return the worker capacity in bytes
   */
  public String getCapacity() {
    return mCapacity;
  }

  /**
   * @return the worker free space as a percentage
   */
  public int getFreeSpacePercent() {
    return mFreePercent;
  }

  /**
   * @return the time of the last worker heartbeat
   */
  public String getLastHeartbeat() {
    return mLastContactSec;
  }

  /**
   * @return the worker host
   */
  public String getHost() {
    return mHost;
  }

  /**
   * @return the worker port
   */
  public int getWebPort() {
    return mWebPort;
  }

  /**
   * @return the worker state
   */
  public String getState() {
    return mWorkerState;
  }

  /**
   * @return the worker uptime
   */
  public String getUptimeClockTime() {
    return mUptimeClockTime;
  }

  /**
   * @return the worker used capacity in bytes
   */
  public String getUsedMemory() {
    return mUsedMemory;
  }

  /**
   * @return the worker used space as a percentage
   */
  public int getUsedSpacePercent() {
    return mUsedPercent;
  }

  /**
   * @return the worker id
   */
  public long getWorkerId() {
    return mWorkerId;
  }

  /**
   * Compare {@link NodeInfo} by lexicographical order of their associated host.
   *
   * @param o the comparison term
   * @return a positive value if {@code this.getHost} is lexicographically "bigger" than
   *         {@code o.getHost}, 0 if the hosts are equal, a negative value otherwise.
   */
  @Override
  public int compareTo(NodeInfo o) {
    if (o == null) {
      return 1;
    } else {
      return this.getHost().compareTo(o.getHost());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof NodeInfo)) {
      return false;
    }
    return this.getHost().equals(((NodeInfo) o).getHost());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.getHost());
  }
}
