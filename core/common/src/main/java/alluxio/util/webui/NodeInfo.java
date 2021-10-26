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

import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.wire.WorkerInfo;

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
  private final long mCapacityBytes;
  private final long mUsedBytes;
  private final int mFreePercent;
  private final int mUsedPercent;
  private final String mUptimeClockTime;
  private final long mWorkerId;
  private final long mBlockCount;

  /**
   * Instantiates a new Node info.
   *
   * @param workerInfo the worker info
   */
  public NodeInfo(WorkerInfo workerInfo) {
    if (!workerInfo.getAddress().getContainerHost().equals("")) {
      mHost = String.format("%s (%s)", workerInfo.getAddress().getHost(),
              workerInfo.getAddress().getContainerHost());
    } else {
      mHost = workerInfo.getAddress().getHost();
    }
    mWebPort = workerInfo.getAddress().getWebPort();
    mLastContactSec = Integer.toString(workerInfo.getLastContactSec());
    mWorkerState = workerInfo.getState();
    mCapacityBytes = workerInfo.getCapacityBytes();
    mUsedBytes = workerInfo.getUsedBytes();
    if (mCapacityBytes != 0) {
      mUsedPercent = (int) (100L * mUsedBytes / mCapacityBytes);
    } else {
      mUsedPercent = 0;
    }
    mFreePercent = 100 - mUsedPercent;
    mUptimeClockTime =
        CommonUtils.convertMsToClockTime(
            System.currentTimeMillis() - workerInfo.getStartTimeMs());
    mWorkerId = workerInfo.getId();
    mBlockCount = workerInfo.getBlockCount();
  }

  /**
   * Gets capacity.
   *
   * @return the worker capacity in bytes
   */
  public String getCapacity() {
    return FormatUtils.getSizeFromBytes(mCapacityBytes);
  }

  /**
   * Gets free space percent.
   *
   * @return the worker free space as a percentage
   */
  public int getFreeSpacePercent() {
    return mFreePercent;
  }

  /**
   * Gets last heartbeat.
   *
   * @return the time of the last worker heartbeat
   */
  public String getLastHeartbeat() {
    return mLastContactSec;
  }

  /**
   * Gets host.
   *
   * @return the worker host
   */
  public String getHost() {
    return mHost;
  }

  /**
   * Gets web port.
   *
   * @return the worker port
   */
  public int getWebPort() {
    return mWebPort;
  }

  /**
   * Gets state.
   *
   * @return the worker state
   */
  public String getState() {
    return mWorkerState;
  }

  /**
   * Gets uptime clock time.
   *
   * @return the worker uptime
   */
  public String getUptimeClockTime() {
    return mUptimeClockTime;
  }

  /**
   * Gets used memory.
   *
   * @return the worker used capacity in bytes
   */
  public String getUsedMemory() {
    return FormatUtils.getSizeFromBytes(mUsedBytes);
  }

  /**
   * Gets used space percent.
   *
   * @return the worker used space as a percentage
   */
  public int getUsedSpacePercent() {
    return mUsedPercent;
  }

  /**
   * Gets worker id.
   *
   * @return the worker id
   */
  public long getWorkerId() {
    return mWorkerId;
  }

  /**
   * Gets worker block count.
   *
   * @return the worker block count
   */
  public long getBlockCount() {
    return mBlockCount;
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
