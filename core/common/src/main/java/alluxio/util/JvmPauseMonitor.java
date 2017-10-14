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

package alluxio.util;

import alluxio.PropertyKey;
import alluxio.Configuration;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Class which sets up a simple thread which runs in a loop sleeping
 * for a short interval of time. If the sleep takes significantly longer
 * than its target time, it implies that the JVM or host machine has
 * paused processing, which may cause other problems. If such a pause is
 * detected, the thread logs a message.
 */
public final class JvmPauseMonitor {
  private static final Log LOG = LogFactory.getLog(
      JvmPauseMonitor.class);

  /** The target sleep time. */
  private final long mGcSleepIntervalMs;

  /** log WARN if we detect a pause longer than this threshold.*/
  private final long mWarnThresholdMs;

  /** log INFO if we detect a pause longer than this threshold. */
  private final long mInfoThresholdMs;

  private long mNumGcWarnThresholdExceeded = 0;
  private long mNumGcInfoThresholdExceeded = 0;
  private long mTotalGcExtraSleepTime = 0;

  private Thread mMonitorThread;
  private volatile boolean mShouldRun = true;

  /**
   * Constructs JvmPauseMonitor.
   */
  public JvmPauseMonitor() {
    mGcSleepIntervalMs = Configuration.getLong(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS);
    mWarnThresholdMs = Configuration.getLong(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS);
    mInfoThresholdMs = Configuration.getLong(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS);
  }

  /**
   * Starts jvm monitor.
   */
  public void start() {
    Preconditions.checkState(mMonitorThread == null,
        "Already started");
    mMonitorThread = new Daemon(new Monitor());
    mMonitorThread.start();
  }

  /**
   * Stops jvm monitor.
   */
  public void stop() {
    mShouldRun = false;
    mMonitorThread.interrupt();
    try {
      mMonitorThread.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * @return boolean if started,false otherwise
   */
  public boolean isStarted() {
    return mMonitorThread != null;
  }

  /**
   * @return mNumGcWarnThresholdExceeded
   */
  public long getNumGcWarnThreadholdExceeded() {
    return mNumGcWarnThresholdExceeded;
  }

  /**
   * @return mNumGcInfoThresholdExceeded
   */
  public long getNumGcInfoThresholdExceeded() {
    return mNumGcInfoThresholdExceeded;
  }

  /**
   * @return mTotalGcExtraSleepTime
   */
  public long getTotalGcExtraSleepTime() {
    return mTotalGcExtraSleepTime;
  }

  private String formatMessage(long extraSleepTime, Map<String, GcTimes> gcTimesAfterSleep,
      Map<String, GcTimes> gcTimesBeforeSleep) {

    Set<String> gcBeanNames = Sets.intersection(
        gcTimesAfterSleep.keySet(),
        gcTimesBeforeSleep.keySet());
    List<String> gcDiffs = Lists.newArrayList();
    for (String name : gcBeanNames) {
      GcTimes diff = gcTimesAfterSleep.get(name).subtract(
          gcTimesBeforeSleep.get(name));
      if (diff.mGcCount != 0) {
        gcDiffs.add("GC pool '" + name + "' had collection(s): "
            + diff.toString());
      }
    }

    String ret = "Detected pause in JVM or host machine (eg GC): "
        + "pause of approximately " + extraSleepTime + "ms\n";
    if (gcDiffs.isEmpty()) {
      ret += "No GCs detected";
    } else {
      ret += Joiner.on("\n").join(gcDiffs);
    }
    return ret;
  }

  private Map<String, GcTimes> getGcTimes() {
    Map<String, GcTimes> map = Maps.newHashMap();
    List<GarbageCollectorMXBean> gcBeans =
        ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      map.put(gcBean.getName(), new GcTimes(gcBean));
    }
    return map;
  }

  private static final class GcTimes {
    private GcTimes(GarbageCollectorMXBean gcBean) {
      mGcCount = gcBean.getCollectionCount();
      mGcTimeMillis = gcBean.getCollectionTime();
    }

    private GcTimes(long count, long time) {
      mGcCount = count;
      mGcTimeMillis = time;
    }

    private GcTimes subtract(GcTimes other) {
      return new GcTimes(this.mGcCount - other.mGcCount,
          mGcTimeMillis - other.mGcTimeMillis);
    }

    @Override
    public String toString() {
      return "count=" + mGcCount + " time=" + mGcTimeMillis + "ms";
    }

    private long mGcCount;
    private long mGcTimeMillis;
  }

  private class Monitor implements Runnable {
    @Override
    public void run() {
      StopWatch sw = new StopWatch();
      Map<String, GcTimes> gcTimesBeforeSleep = getGcTimes();
      while (mShouldRun) {
        sw.reset().start();
        try {
          Thread.sleep(mGcSleepIntervalMs);
        } catch (InterruptedException ie) {
          return;
        }
        long extraSleepTime = sw.now(TimeUnit.MILLISECONDS) - mGcSleepIntervalMs;
        Map<String, GcTimes> gcTimesAfterSleep = getGcTimes();

        if (extraSleepTime > mWarnThresholdMs) {
          ++mNumGcWarnThresholdExceeded;
          LOG.warn(formatMessage(
              extraSleepTime, gcTimesAfterSleep, gcTimesBeforeSleep));
        } else if (extraSleepTime > mInfoThresholdMs) {
          ++mNumGcInfoThresholdExceeded;
          LOG.info(formatMessage(
              extraSleepTime, gcTimesAfterSleep, gcTimesBeforeSleep));
        }
        mTotalGcExtraSleepTime += extraSleepTime;
        gcTimesBeforeSleep = gcTimesAfterSleep;
      }
    }
  }
}
