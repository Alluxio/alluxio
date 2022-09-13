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

package alluxio.master.throttle;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterProcess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The system monitor.
 */
@NotThreadSafe
public class SystemMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(SystemMonitor.class);

  private List<ServerIndicator> mServerIndicatorsList;
  private ServerIndicator mAggregatedServerIndicators;

  private FileSystemIndicator mCurrentFileSystemIndicators;
  private FileSystemIndicator mPrevFileSystemIndicators;
  private FileSystemIndicator mDeltaFilesystemIndicators;
  private final int mMaxNumberOfSnapshot;

  private long mCurrentThresholdTimeIn42Sec;
  private ServerIndicator mPitThresholdNormal;
  private ServerIndicator mAggregateThresholdNormal;
  private ServerIndicator mPitThresholdLoad;
  private ServerIndicator mAggregateThresholdLoad;
  private ServerIndicator mPitThresholdStress;
  private ServerIndicator mAggregateThresholdStress;

  private SystemStatus mCurrentSystemStatus;

  private PitInfo mPrevPitInfo;

  /**
   * Pit information.
   */
  private static class PitInfo {
    private SystemStatus mSystemStatus;
    private long mPitTime;
    private long mJVMPauseTime;

    /**
     * Constructor.
     *
     * @param systemStatus the system status
     * @param pitTime the pit time
     * @param jvmPauseTime the JVM pause time
     */
    public PitInfo(SystemStatus systemStatus, long pitTime, long jvmPauseTime) {
      mSystemStatus = systemStatus;
      mPitTime = pitTime;
      mJVMPauseTime = jvmPauseTime;
    }

    /**
     * @return the pit time
     */
    public long getPitTime() {
      return mPitTime;
    }

    /**
     * @return the JVP pause time
     */
    public long getJVMPauseTime() {
      return mJVMPauseTime;
    }

    /**
     * @return the system status
     */
    public SystemStatus getSystemStatus() {
      return mSystemStatus;
    }

    /**
     * Sts the system status.
     *
     * @param systemStatus the system status
     */
    public void setSystemStatus(SystemStatus systemStatus) {
      mSystemStatus = systemStatus;
    }

    /**
     * Sets the JVM pause time.
     *
     * @param jvmPauseTime the JVM pause time
     */
    public void setJVMPauseTime(long jvmPauseTime) {
      mJVMPauseTime = jvmPauseTime;
    }

    /**
     * Sets the pit time.
     *
     * @param pitTime the pit time
     */
    public void setPitTime(long pitTime) {
      mPitTime = pitTime;
    }
  }

  /**
   * The system status.
   */
  public enum SystemStatus {
    IDLE,
    ACTIVE,
    STRESSED,
    OVERLOADED,
  }

  /**
   * The status transition.
   */
  public enum StatusTransition {
    UNCHANGED,
    ESCALATE,
    DEESCALATE,
  }

  /**
   * The system monitor constructor.
   *
   * @param masterProcess the master process
   */
  public SystemMonitor(MasterProcess masterProcess) {
    mServerIndicatorsList = new LinkedList<>();
    mCurrentSystemStatus = SystemStatus.IDLE;

    // Configurable
    mMaxNumberOfSnapshot = Configuration.getInt(PropertyKey.MASTER_THROTTLE_OBSERVED_PIT_NUMBER);

    // covert the percentage of memory usage to usedHeap
    mCurrentThresholdTimeIn42Sec = 0;
    reInitTheThresholds();
    mPrevPitInfo = new PitInfo(mCurrentSystemStatus,
        ServerIndicator.getSystemTotalJVMPauseTime(), System.currentTimeMillis());
  }

  private void reInitTheThresholds() {
    long current = (System.nanoTime() >> 32);
    if (current == mCurrentThresholdTimeIn42Sec) {
      return;
    }
    mCurrentThresholdTimeIn42Sec = current;

    // covert the percentage of memory usage to usedHeap
    // mPitThresholdNormal;
    mPitThresholdNormal = ServerIndicator.createThresholdIndicator(
        Configuration.getDouble(PropertyKey.MASTER_THROTTLE_NORMAL_HEAP_USED_RATIO),
        Configuration.getDouble(PropertyKey.MASTER_THROTTLE_NORMAL_CPU_LOAD_RATIO),
        Configuration.getMs(PropertyKey.MASTER_THROTTLE_NORMAL_HEAP_GC_TIME),
        Configuration.getInt(PropertyKey.MASTER_THROTTLE_NORMAL_RPC_QUEUE_SIZE));
    // mPitThresholdLoad;
    mPitThresholdLoad = ServerIndicator.createThresholdIndicator(
        Configuration.getDouble(PropertyKey.MASTER_THROTTLE_LOAD_HEAP_USED_RATIO),
        Configuration.getDouble(PropertyKey.MASTER_THROTTLE_LOAD_CPU_LOAD_RATIO),
        Configuration.getMs(PropertyKey.MASTER_THROTTLE_LOAD_HEAP_GC_TIME),
        Configuration.getInt(PropertyKey.MASTER_THROTTLE_LOAD_RPC_QUEUE_SIZE));
    // mPitThresholdStress;
    mPitThresholdStress = ServerIndicator.createThresholdIndicator(
        Configuration.getDouble(PropertyKey.MASTER_THROTTLE_STRESS_HEAP_USED_RATIO),
        Configuration.getDouble(PropertyKey.MASTER_THROTTLE_STRESS_CPU_LOAD_RATIO),
        Configuration.getMs(PropertyKey.MASTER_THROTTLE_STRESS_HEAP_GC_TIME),
        Configuration.getInt(PropertyKey.MASTER_THROTTLE_STRESS_RPC_QUEUE_SIZE));
    mAggregateThresholdNormal = new ServerIndicator(mPitThresholdNormal, mMaxNumberOfSnapshot);
    mAggregateThresholdLoad = new ServerIndicator(mPitThresholdLoad, mMaxNumberOfSnapshot);
    mAggregateThresholdStress = new ServerIndicator(mPitThresholdStress, mMaxNumberOfSnapshot);
  }

  /**
   * Main run.
   */
  public void run() {
    // the indicator pit time is not necessary accurate,
    long pitTimeMS = System.currentTimeMillis();
    collectIndicators(pitTimeMS);
    checkAndBackPressure();
    mPrevPitInfo.setPitTime(pitTimeMS);
    mPrevPitInfo.setSystemStatus(mCurrentSystemStatus);
    mPrevPitInfo.setJVMPauseTime(mServerIndicatorsList
        .get(mServerIndicatorsList.size() - 1).getPITTotalJVMPauseTimeMS());
  }

  private void collectIndicators(long pitTimeMS) {
    collectServerIndicators(pitTimeMS);
    collectFileSystemIndicators(pitTimeMS);
  }

  private void produceDeltaFilesystemIndicators() {
    // Only be able to calculate the delta if the two filesystem indicators are fetched
    // continuously, this means both previous and current status should be either STRESSED
    // or OVERLOADED
    if ((mPrevPitInfo.getSystemStatus() == SystemStatus.STRESSED
        || mPrevPitInfo.getSystemStatus() == SystemStatus.OVERLOADED)
        && (mCurrentSystemStatus == SystemStatus.STRESSED
        || mCurrentSystemStatus == SystemStatus.OVERLOADED)
        && mPrevFileSystemIndicators != null
        && mCurrentFileSystemIndicators != null) {
      mDeltaFilesystemIndicators = new FileSystemIndicator(mCurrentFileSystemIndicators);
      mDeltaFilesystemIndicators.deltaTo(mPrevFileSystemIndicators);
    } else {
      mDeltaFilesystemIndicators = null;
    }
  }

  private void collectFileSystemIndicators(long snapshotTimeMS) {
    mPrevFileSystemIndicators = mCurrentFileSystemIndicators;
    if (mCurrentSystemStatus == SystemStatus.IDLE
        || mCurrentSystemStatus == SystemStatus.ACTIVE) {
      // don't collect information in case of IDLE and ACTIVE
      mCurrentFileSystemIndicators = null;
      mDeltaFilesystemIndicators = null;
      return;
    }

    FileSystemIndicator fileSystemIndicator = new FileSystemIndicator();
    fileSystemIndicator.setPitTimeMS(snapshotTimeMS);

    mCurrentFileSystemIndicators = fileSystemIndicator;
    produceDeltaFilesystemIndicators();
  }

  private void collectServerIndicators(long snapshotTimeMS) {
    ServerIndicator serverIndicator
        = ServerIndicator.createFromMetrics(
        mPrevPitInfo != null ? mPrevPitInfo.getJVMPauseTime() : 0);
    serverIndicator.setPitTimeMS(snapshotTimeMS);

    mServerIndicatorsList.add(serverIndicator);
    if (mServerIndicatorsList.size() > mMaxNumberOfSnapshot && !mServerIndicatorsList.isEmpty()) {
      ServerIndicator toBeRemoved = mServerIndicatorsList.get(0);
      if (mAggregatedServerIndicators != null) {
        mAggregatedServerIndicators.reduction(toBeRemoved);
      }
      mServerIndicatorsList.remove(0);
    }

    if (mAggregatedServerIndicators == null) {
      mAggregatedServerIndicators = new ServerIndicator(serverIndicator);
    } else {
      mAggregatedServerIndicators.addition(serverIndicator);
    }
  }

  private StatusTransition statusCheck(ServerIndicator aggregatedServerIndicator,
      ServerIndicator pitServerIndicator,
      ServerIndicator lowBoundaryPitIndicator,
      ServerIndicator highBoundaryPitIndicator,
      ServerIndicator lowBoundaryAggregateIndicator,
      ServerIndicator highBoundaryAggregateIndicator) {
    // So far it is not clear how to evaluate the indicators, but just start with simple.
    // When it is clear, more rules can be added

    // The pitServerIndicator shows the current status, it reflects the current status.
    // so it is checked first. If it is in reasonable range, the system should be
    if (lowBoundaryPitIndicator != null
        && pitServerIndicator.getHeapUsed() < lowBoundaryPitIndicator.getHeapUsed()) {
      return StatusTransition.DEESCALATE;
    }

    if (highBoundaryPitIndicator != null
        && pitServerIndicator.getHeapUsed() < highBoundaryPitIndicator.getHeapUsed()) {
      // if the heap usage is not crossing the higher boundary, no change
      return StatusTransition.UNCHANGED;
    }

    if (highBoundaryAggregateIndicator != null
        && aggregatedServerIndicator.getHeapUsed()
        > highBoundaryAggregateIndicator.getHeapUsed()) {
      return StatusTransition.ESCALATE;
    }

    return StatusTransition.UNCHANGED;
  }

  private void checkAndBackPressure() {
    if (mServerIndicatorsList.isEmpty()) {
      return;
    }

    reInitTheThresholds();

    ServerIndicator pitIndicator = mServerIndicatorsList.get(mServerIndicatorsList.size() - 1);
    StatusTransition statusTransition = StatusTransition.UNCHANGED;

    switch (mCurrentSystemStatus) {
      case IDLE:
        statusTransition = statusCheck(mAggregatedServerIndicators, pitIndicator, null,
            mPitThresholdNormal, null, mAggregateThresholdNormal);
        if (statusTransition == StatusTransition.ESCALATE) {
          mCurrentSystemStatus = SystemStatus.ACTIVE;
        }
        break;
      case ACTIVE:
        statusTransition = statusCheck(mAggregatedServerIndicators, pitIndicator,
            mPitThresholdNormal, mPitThresholdLoad,
            mAggregateThresholdNormal, mAggregateThresholdLoad);
        if (statusTransition == StatusTransition.DEESCALATE) {
          mCurrentSystemStatus = SystemStatus.IDLE;
        } else if (statusTransition == StatusTransition.ESCALATE) {
          mCurrentSystemStatus = SystemStatus.STRESSED;
        }
        break;
      case STRESSED:
        statusTransition = statusCheck(mAggregatedServerIndicators, pitIndicator,
            mPitThresholdLoad, mPitThresholdStress,
            mAggregateThresholdLoad, mAggregateThresholdStress);
        if (statusTransition == StatusTransition.DEESCALATE) {
          mCurrentSystemStatus = SystemStatus.ACTIVE;
        } else if (statusTransition == StatusTransition.ESCALATE) {
          mCurrentSystemStatus = SystemStatus.OVERLOADED;
        }
        break;
      case OVERLOADED:
        statusTransition = statusCheck(mAggregatedServerIndicators, pitIndicator,
            mPitThresholdStress, null,
            mAggregateThresholdStress, null);
        if (statusTransition == StatusTransition.DEESCALATE) {
          mCurrentSystemStatus = SystemStatus.STRESSED;
        }
        break;
      default:
    }

    if (mCurrentSystemStatus == SystemStatus.STRESSED
        || mCurrentSystemStatus == SystemStatus.OVERLOADED) {
      // print logs
      LOG.warn("The system status is {}, now in {}, related Server aggregate indicators:{},"
              + " pit indicators:{}",
          statusTransition, mCurrentSystemStatus, mAggregatedServerIndicators, pitIndicator);
      if (mDeltaFilesystemIndicators != null) {
        LOG.warn("The delta filesystem indicators {}", mDeltaFilesystemIndicators);
      }
    }

    LOG.debug("The system status is {}, now in {}, related Server aggregate indicators:{},"
            + " pit indicators:{}",
        statusTransition, mCurrentSystemStatus, mAggregatedServerIndicators, pitIndicator);

    LOG.debug("The delta filesystem indicators {}",
        mDeltaFilesystemIndicators == null ? "" : mDeltaFilesystemIndicators);

    // TODO(yyong) comment it out so far
    /**
    switch (mCurrentSystemStatus) {
      case OVERLOADED:
        SystemThrottle.SYSTEM_THROTTLE.setBackgroundJobThrottle(true);
        SystemThrottle.SYSTEM_THROTTLE.setForegroundJobThrottle(true);
        break;
      case STRESSED:
        SystemThrottle.SYSTEM_THROTTLE.setBackgroundJobThrottle(true);
        SystemThrottle.SYSTEM_THROTTLE.setForegroundJobThrottle(false);
        break;
      case IDLE:
      case ACTIVE:
        SystemThrottle.SYSTEM_THROTTLE.setBackgroundJobThrottle(false);
        SystemThrottle.SYSTEM_THROTTLE.setForegroundJobThrottle(false);
        break;
      default:
    }
     **/
  }
}

