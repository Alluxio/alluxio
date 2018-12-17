package alluxio.util.webui;

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

  public NodeInfo(WorkerInfo workerInfo) {
    mHost = workerInfo.getAddress().getHost();
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
        WebUtils.convertMsToShortClockTime(
            System.currentTimeMillis() - workerInfo.getStartTimeMs());
    mWorkerId = workerInfo.getId();
  }

  /**
   * @return the worker capacity in bytes
   */
  public String getCapacity() {
    return FormatUtils.getSizeFromBytes(mCapacityBytes);
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
    return FormatUtils.getSizeFromBytes(mUsedBytes);
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
