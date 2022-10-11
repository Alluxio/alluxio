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
import alluxio.metrics.MetricsSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nullable;
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
  private ServerIndicator mPitThresholdActive;
  private ServerIndicator mAggregateThresholdActive;
  private ServerIndicator mPitThresholdStressed;
  private ServerIndicator mAggregateThresholdStressed;
  private ServerIndicator mPitThresholdOverloaded;
  private ServerIndicator mAggregateThresholdOverloaded;

  private volatile SystemStatus mCurrentSystemStatus;

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

    MetricsSystem.registerGaugeIfAbsent(
        MetricsSystem.getMetricName("system.status"),
        () -> mCurrentSystemStatus);
  }

  private void reInitTheThresholds() {
    // Only update thresholds every 42 seconds
    long current = (System.nanoTime() >> 32);
    if (current == mCurrentThresholdTimeIn42Sec) {
      return;
    }
    mCurrentThresholdTimeIn42Sec = current;

    // covert the percentage of memory usage to usedHeap
    // mPitThresholdActive;
    mPitThresholdActive = ServerIndicator.createThresholdIndicator(0,
        Configuration.getDouble(PropertyKey.MASTER_THROTTLE_ACTIVE_HEAP_USED_RATIO),
        Configuration.getDouble(PropertyKey.MASTER_THROTTLE_ACTIVE_CPU_LOAD_RATIO),
        Configuration.getMs(PropertyKey.MASTER_THROTTLE_ACTIVE_HEAP_GC_TIME),
        Configuration.getInt(PropertyKey.MASTER_THROTTLE_ACTIVE_RPC_QUEUE_SIZE));
    // mPitThresholdStressed;
    mPitThresholdStressed = ServerIndicator.createThresholdIndicator(0,
        Configuration.getDouble(PropertyKey.MASTER_THROTTLE_STRESSED_HEAP_USED_RATIO),
        Configuration.getDouble(PropertyKey.MASTER_THROTTLE_STRESSED_CPU_LOAD_RATIO),
        Configuration.getMs(PropertyKey.MASTER_THROTTLE_STRESSED_HEAP_GC_TIME),
        Configuration.getInt(PropertyKey.MASTER_THROTTLE_STRESSED_RPC_QUEUE_SIZE));
    // mPitThresholdOverloaded;
    mPitThresholdOverloaded = ServerIndicator.createThresholdIndicator(0,
        Configuration.getDouble(PropertyKey.MASTER_THROTTLE_OVERLOADED_HEAP_USED_RATIO),
        Configuration.getDouble(PropertyKey.MASTER_THROTTLE_OVERLOADED_CPU_LOAD_RATIO),
        Configuration.getMs(PropertyKey.MASTER_THROTTLE_OVERLOADED_HEAP_GC_TIME),
        Configuration.getInt(PropertyKey.MASTER_THROTTLE_OVERLOADED_RPC_QUEUE_SIZE));
    mAggregateThresholdActive = new ServerIndicator(mPitThresholdActive, mMaxNumberOfSnapshot);
    mAggregateThresholdStressed
        = new ServerIndicator(mPitThresholdStressed, mMaxNumberOfSnapshot);
    mAggregateThresholdOverloaded
        = new ServerIndicator(mPitThresholdOverloaded, mMaxNumberOfSnapshot);
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

  /**
   * Checks whether to escalate, deescalate or unchanged, if the pit low boundary is null,
   * not deescalated, and if the high aggregated boundary is null, not escalated.
   *
   * @param aggregatedServerIndicator the aggregated server indicator
   * @param pitServerIndicator the pit server indicator
   * @param lowBoundaryPitIndicator the low pit boundary indicator
   * @param highBoundaryPitIndicator the high pit boundary indicator
   * @param lowBoundaryAggregateIndicator the low aggregated boundary indicator
   * @param highBoundaryAggregateIndicator the high aggregated boundary indicator
   * @return Whether to escalate, deescalate or no change
   */
  private StatusTransition boundaryCheck(ServerIndicator aggregatedServerIndicator,
      ServerIndicator pitServerIndicator,
      @Nullable ServerIndicator lowBoundaryPitIndicator,
      @Nullable ServerIndicator highBoundaryPitIndicator,
      @Nullable ServerIndicator lowBoundaryAggregateIndicator,
      @Nullable ServerIndicator highBoundaryAggregateIndicator) {
    // Just start with simple.
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

    // Status transition would look like: IDLE <--> ACTIVE <--> STRESSED <--> OVERLOADED
    // There are three set of thresholds,
    //        pit ACTIVE threshold, aggregated ACTIVE threshold
    //        pit STRESSED threshold, aggregated STRESSED threshold
    //        pit OVERLOADED threshold, aggregated OVERLOADED threshold
    // Every status would have two set of boundaries: low boundary and up boundary
    // IDLE:
    //        low boundary: null, null
    //        up boundary: pit STRESSED threshold, aggregated STRESSED threshold
    // ACTIVE:
    //        low boundary: pit ACTIVE threshold, aggregated ACTIVE threshold
    //        up boundary: pit STRESSED threshold, aggregated STRESSED threshold
    // STRESSED:
    //        low boundary: pit STRESSED threshold, aggregated STRESSED threshold
    //        up boundary: pit OVERLOADED threshold, aggregated OVERLOADED threshold
    // OVERLOADED:
    //        low boundary: pit OVERLOADED threshold, aggregated OVERLOADED threshold
    //        up boundary: null, null
    switch (mCurrentSystemStatus) {
      case IDLE:
        statusTransition = boundaryCheck(mAggregatedServerIndicators, pitIndicator, null,
            mPitThresholdActive, null, mAggregateThresholdActive);
        if (statusTransition == StatusTransition.ESCALATE) {
          mCurrentSystemStatus = SystemStatus.ACTIVE;
        }
        break;
      case ACTIVE:
        statusTransition = boundaryCheck(mAggregatedServerIndicators, pitIndicator,
            mPitThresholdActive, mPitThresholdStressed,
            mAggregateThresholdActive, mAggregateThresholdStressed);
        if (statusTransition == StatusTransition.DEESCALATE) {
          mCurrentSystemStatus = SystemStatus.IDLE;
        } else if (statusTransition == StatusTransition.ESCALATE) {
          mCurrentSystemStatus = SystemStatus.STRESSED;
        }
        break;
      case STRESSED:
        statusTransition = boundaryCheck(mAggregatedServerIndicators, pitIndicator,
            mPitThresholdStressed, mPitThresholdOverloaded,
            mAggregateThresholdStressed, mAggregateThresholdOverloaded);
        if (statusTransition == StatusTransition.DEESCALATE) {
          mCurrentSystemStatus = SystemStatus.ACTIVE;
        } else if (statusTransition == StatusTransition.ESCALATE) {
          mCurrentSystemStatus = SystemStatus.OVERLOADED;
        }
        break;
      case OVERLOADED:
        statusTransition = boundaryCheck(mAggregatedServerIndicators, pitIndicator,
            mPitThresholdOverloaded, null,
            mAggregateThresholdOverloaded, null);
        if (statusTransition == StatusTransition.DEESCALATE) {
          mCurrentSystemStatus = SystemStatus.STRESSED;
        }
        break;
      default:
    }

    if (mCurrentSystemStatus == SystemStatus.STRESSED
        || mCurrentSystemStatus == SystemStatus.OVERLOADED) {
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

