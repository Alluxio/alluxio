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
import sun.misc.SharedSecrets;

/**
 * The server indicators.
 */
public class ServerIndicator {
  private static final Logger LOG = LoggerFactory.getLogger(ServerIndicator.class);
  private Long mHeapMax;
  private Long mHeapUsed;
  private long mDirectMemUsed;
  private Double mCpuLoad;

  private long mTotalJVMPauseTimeMS;
  private long mPITTotalJVMPauseTimeMS;
  private long mRpcQueueSize;

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
      return (long) (MetricsSystem.METRIC_REGISTRY
          .gauge(MetricsMonitorUtils.ServerGaugeName.TOTAL_EXTRA_TIME, null).getValue());
    }
    return 0;
  }

  /**
   * creates server indicator based on current status.
   *
   * @param prevTotalJVMPauseTime the previous total JVM pause time
   * @return the server indicator
   */
  public static ServerIndicator createFromMetrics(long prevTotalJVMPauseTime) {
    long pitTotalJVMPauseTime = 0;
    long totalJVMPauseTime = 0;
    long rpcQueueSize = 0;

    if (Configuration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED)) {
      pitTotalJVMPauseTime = (long) (MetricsSystem.METRIC_REGISTRY
          .gauge(MetricsMonitorUtils.ServerGaugeName.TOTAL_EXTRA_TIME, null).getValue());
      totalJVMPauseTime = pitTotalJVMPauseTime - prevTotalJVMPauseTime;
    }
    try {
      rpcQueueSize = (long) (MetricsSystem.METRIC_REGISTRY
          .gauge(MetricsMonitorUtils.ServerGaugeName.RPC_QUEUE_LENGTH, null).getValue());
    } catch (NullPointerException e) {
      // ignore
    }

    return new ServerIndicator(SharedSecrets.getJavaNioAccess()
        .getDirectBufferPool().getMemoryUsed(),
        (long) (MetricsSystem.METRIC_REGISTRY
        .gauge(MetricsMonitorUtils.MemoryGaugeName.HEAP_MAX, null).getValue()),
        (long) (MetricsSystem.METRIC_REGISTRY
        .gauge(MetricsMonitorUtils.MemoryGaugeName.HEAP_USED, null).getValue()),
        (double) (MetricsSystem.METRIC_REGISTRY
            .gauge(MetricsMonitorUtils.OSGaugeName.OS_CPU_LOAD, null).getValue()),
        totalJVMPauseTime, pitTotalJVMPauseTime, rpcQueueSize, System.currentTimeMillis());
  }

  /**
   * Creates a threshold indicator object.
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
    long heapMax = (long) (MetricsSystem.METRIC_REGISTRY
        .gauge(MetricsMonitorUtils.MemoryGaugeName.HEAP_MAX, null).getValue());
    long heapUsed = (long) (ratioOfUsedHeap * heapMax);

    if (Configuration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED)) {
      pitTotalJVMPauseTimeMS = (long) (MetricsSystem.METRIC_REGISTRY
          .gauge(MetricsMonitorUtils.ServerGaugeName.TOTAL_EXTRA_TIME, null).getValue());
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
   * Gets the delta the server indicator based on server indicator.
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

