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
import alluxio.metrics.MetricsSystem;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The server indicators.
 * It can be used as a point-in-time indicator, an aggregated indicator, a single threshold
 * indicator or aggregated threshold.
 * For several PIT indicators can be aggregated, the addition function can be used
 * for that.
 * For one indicator can also be reduced, the reduction function can be used.
 * One example is the sliding window:
 *  aggregated indicator1 = ( pit1 + pit2 + pit3)
 *  aggregated indicator2 = (aggregated indicator1 + pit4 - pit1)
 */
public class ServerIndicator {
  private static final Logger LOG = LoggerFactory.getLogger(ServerIndicator.class);
  private long mHeapMax;
  private long mHeapUsed;
  private long mDirectMemUsed;
  private double mCpuLoad;

  private long mTotalJVMPauseTimeMS;
  private long mPITTotalJVMPauseTimeMS;
  private long mRpcQueueSize;

  // The time the point-in-time indicator generated
  // It is not useful if it is an aggregated one
  private long mPitTimeMS;

  /**
   * @param directMemUsed  the allocated direct memory
   * @param heapMax the max heap
   * @param heapUsed the used heap
   * @param cpuLoad the cpu load
   * @param totalJVMPauseTimeMS the total JVM pause time since previous PIT
   * @param pitTotalJVMPauseTimeMS the pit total JVM pause time
   * @param rpcQueueSize the pit rpc queue size
   * @param snapshotTimeMS the pit time
   */
  public ServerIndicator(long directMemUsed, long heapMax, long heapUsed, double cpuLoad,
      long totalJVMPauseTimeMS, long pitTotalJVMPauseTimeMS, long rpcQueueSize,
      long snapshotTimeMS) {
    mDirectMemUsed = directMemUsed;
    mHeapMax = heapMax;
    mHeapUsed = heapUsed;
    mCpuLoad = cpuLoad;
    mTotalJVMPauseTimeMS = totalJVMPauseTimeMS;
    mPITTotalJVMPauseTimeMS = pitTotalJVMPauseTimeMS;
    mRpcQueueSize = rpcQueueSize;
    mPitTimeMS = snapshotTimeMS;
  }

  /**
   * @param serverIndicator the server indicator
   */
  public ServerIndicator(ServerIndicator serverIndicator) {
    this(serverIndicator, 1);
  }

  /**
   * The scaled server indicator according to the multiple.
   * The threshold is set for single point in time value, and sometimes the sliding window is used
   * to check if the threshold is crossed, in that case multiple times of single pit threshold is
   * required. Eg, if the sliding window side is 3, (pit1, pit2, pit3), (pit2, pit3, pit4) ... are
   * calculated. The threshold value * 3 would be used to check the value of sliding window. This
   * constructor is helping generate the threshold of sliding window.
   *
   * @param serverIndicator the base server indicator
   * @param multiple the times
   */
  public ServerIndicator(ServerIndicator serverIndicator, int multiple) {
    Preconditions.checkNotNull(serverIndicator, "serverIndicator");
    mDirectMemUsed = serverIndicator.mDirectMemUsed * multiple;
    mHeapMax = serverIndicator.mHeapMax;
    mHeapUsed = serverIndicator.mHeapUsed * multiple;
    mCpuLoad = serverIndicator.mCpuLoad * multiple;
    mTotalJVMPauseTimeMS = serverIndicator.mTotalJVMPauseTimeMS * multiple;
    mPITTotalJVMPauseTimeMS = serverIndicator.mPITTotalJVMPauseTimeMS;
    mRpcQueueSize = serverIndicator.mRpcQueueSize * multiple;
    mPitTimeMS = serverIndicator.mPitTimeMS;
  }

  /**
   * @return current total JVM pause time
   */
  public static long getSystemTotalJVMPauseTime() {
    if (Configuration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED)) {
      return (long) getMetrics(MetricsMonitorUtils.ServerGaugeName.TOTAL_EXTRA_TIME, 0);
    }
    return 0;
  }

  /**
   * Creates a server indicator based on current status.
   *
   * @param prevTotalJVMPauseTime the previous total JVM pause time
   * @return the server indicator
   */
  public static ServerIndicator createFromMetrics(long prevTotalJVMPauseTime) {
    long pitTotalJVMPauseTime = 0;
    long totalJVMPauseTime = 0;
    if (Configuration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED)) {
      pitTotalJVMPauseTime = (long) getMetrics(MetricsMonitorUtils.ServerGaugeName.TOTAL_EXTRA_TIME,
          0);
      totalJVMPauseTime = pitTotalJVMPauseTime - prevTotalJVMPauseTime;
    }

    long rpcQueueSize = getMetrics(MetricsMonitorUtils.ServerGaugeName.RPC_QUEUE_LENGTH, 0);

    return new ServerIndicator((long) getMetrics(
        MetricsSystem.getMetricName(MetricsMonitorUtils.MemoryGaugeName.DIRECT_MEM_USED), 0),
        (long) getMetrics(MetricsMonitorUtils.MemoryGaugeName.HEAP_MAX, 0),
        (long) getMetrics(MetricsMonitorUtils.MemoryGaugeName.HEAP_USED, 0),
        (double) getMetrics(MetricsMonitorUtils.OSGaugeName.OS_CPU_LOAD, 0),
        totalJVMPauseTime, pitTotalJVMPauseTime, rpcQueueSize, System.currentTimeMillis());
  }

  private static <T> T getMetrics(String name, T value) {
    try {
      return (T) (MetricsSystem.METRIC_REGISTRY
          .gauge(name, null).getValue());
    } catch (Exception e) {
      return value;
    }
  }

  /**
   * Creates a threshold indicator object, the parameters are set according the input values.
   *
   * @param directMemUsed the used direct mem
   * @param ratioOfUsedHeap the ratio of used heap
   * @param cpuLoad the cpu load
   * @param totalJVMPauseTimeMS the total JVM pause time
   * @param rpcQueueSize the rpc queue size
   * @return  the threshold indicator object
   */
  public static ServerIndicator createThresholdIndicator(long directMemUsed,
      double ratioOfUsedHeap, double cpuLoad, long totalJVMPauseTimeMS, long rpcQueueSize) {
    long pitTotalJVMPauseTimeMS = 0;
    long heapMax = (long) getMetrics(MetricsMonitorUtils.MemoryGaugeName.HEAP_MAX, 0);
    long heapUsed = (long) (ratioOfUsedHeap * heapMax);

    if (Configuration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED)) {
      pitTotalJVMPauseTimeMS
          = (long) getMetrics(MetricsMonitorUtils.ServerGaugeName.TOTAL_EXTRA_TIME, 0);
    }
    return new ServerIndicator(directMemUsed, heapMax,
        heapUsed, cpuLoad, totalJVMPauseTimeMS, pitTotalJVMPauseTimeMS,
        rpcQueueSize, System.currentTimeMillis());
  }

  /**
   * @return the direct mem used
   */
  public long getDirectMemUsed() {
    return mDirectMemUsed;
  }

  /**
   * @return the max heap
   */
  public long getHeapMax() {
    return mHeapMax;
  }

  /**
   * @return the used heap
   */
  public long getHeapUsed() {
    return mHeapUsed;
  }

  /**
   * @return the cpu load
   */
  public double getCpuLoad() {
    return mCpuLoad;
  }

  /**
   * @return the total JVM pause time
   */
  public long getTotalJVMPauseTimeMS() {
    return mTotalJVMPauseTimeMS;
  }

  /**
   * @return the PIT total JVM pause time
   */
  public long getPITTotalJVMPauseTimeMS() {
    return mPITTotalJVMPauseTimeMS;
  }

  /**
   * @return the rpc queue size
   */
  public long getRpcQueueSize() {
    return mRpcQueueSize;
  }

  /**
   * @return the time to measure the indicators
   */
  public long getPitTimeMS() {
    return mPitTimeMS;
  }

  /**
   * Sets the pit time.
   *
   * @param pitTimeMS the pit time
   */
  public void setPitTimeMS(long pitTimeMS) {
    mPitTimeMS = pitTimeMS;
  }

  /**
   * Addition the server indicator object.
   *
   * @param serverIndicator the server indicator
   */
  public void addition(ServerIndicator serverIndicator) {
    // This is to record the aggregated time
    mDirectMemUsed += serverIndicator.getDirectMemUsed();
    mRpcQueueSize += serverIndicator.getRpcQueueSize();
    mCpuLoad += serverIndicator.getCpuLoad();
    mHeapUsed += serverIndicator.getHeapUsed();
    mTotalJVMPauseTimeMS += serverIndicator.getTotalJVMPauseTimeMS();
  }

  /**
   * Gets the server indicator after reducing the serverIndicator.
   * It can be used in sliding window calculation, eg:
   * Aggregated1 = indicator1  + indicator2 + indicator3
   * Aggregated2 = Aggregated1 - indicator1 + indicator4
   *
   * @param serverIndicator the base server indicator
   */
  public void reduction(ServerIndicator serverIndicator) {
    mDirectMemUsed = mDirectMemUsed > serverIndicator.mDirectMemUsed
        ? mDirectMemUsed - serverIndicator.mDirectMemUsed : 0;
    mRpcQueueSize = mRpcQueueSize > serverIndicator.mRpcQueueSize
        ? mRpcQueueSize - serverIndicator.mRpcQueueSize : 0;
    mCpuLoad = Double.compare(mCpuLoad, serverIndicator.mCpuLoad) > 0
        ? mCpuLoad - serverIndicator.mCpuLoad : 0;
    mHeapUsed = mHeapUsed > serverIndicator.mHeapUsed
        ? mHeapUsed - serverIndicator.mHeapUsed : 0;
    mTotalJVMPauseTimeMS = mTotalJVMPauseTimeMS > serverIndicator.mTotalJVMPauseTimeMS
        ? mTotalJVMPauseTimeMS - serverIndicator.mTotalJVMPauseTimeMS : 0;
  }

  /**
   * @return the string object
   */
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("directMemUsed", mDirectMemUsed)
        .add("heapMax", mHeapMax)
        .add("heapUsed", mHeapUsed)
        .add("cpuLoad", mCpuLoad)
        .add("pitTotalJVMPauseTimeMS", mPITTotalJVMPauseTimeMS)
        .add("totalJVMPauseTimeMS", mTotalJVMPauseTimeMS)
        .add("rpcQueueSize", mRpcQueueSize)
        .add("pitTimeMS", mPitTimeMS)
        .toString();
  }
}
