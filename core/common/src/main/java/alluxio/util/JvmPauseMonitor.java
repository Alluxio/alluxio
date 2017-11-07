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
import com.google.common.base.Stopwatch;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Class to monitor JVM with a daemon thread, the thread sleep period of time
 * and get the true time the sleep takes. If it is longer than it should be, the
 * JVM has paused processing. Then log it into different level.
 */
@NotThreadSafe
public final class JvmPauseMonitor {
  private static final Log LOG = LogFactory.getLog(JvmPauseMonitor.class);

  /** The time to sleep. */
  private final long mGcSleepIntervalMs;

  /** Extra sleep time longer than this threshold, log WARN. */
  private final long mWarnThresholdMs;

  /** Extra sleep time longer than this threshold, log INFO. */
  private final long mInfoThresholdMs;

  /** Times extra sleep time exceed WARN. */
  private long mWarnTimeExceeded = 0;
  /** Times extra sleep time exceed INFO. */
  private long mInfoTimeExceeded = 0;
  /** Total extra sleep time. */
  private long mTotalExtraTimeMs = 0;

  private Thread mJvmMonitorThread;

  /**
   * Constructs JvmPauseMonitor.
   */
  public JvmPauseMonitor() {
    mGcSleepIntervalMs = Configuration.getMs(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS);
    mWarnThresholdMs = Configuration.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS);
    mInfoThresholdMs = Configuration.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS);
  }

  /**
   * Starts jvm monitor thread.
   */
  public void start() {
    Preconditions.checkState(mJvmMonitorThread == null, "JVM monitor thread already started");
    mJvmMonitorThread = new GcMonitor();
    mJvmMonitorThread.start();
  }

  /**
   * Stops jvm monitor.
   */
  public void stop() {
    Preconditions.checkState(mJvmMonitorThread != null,
        "JVM monitor thread does not start");
    mJvmMonitorThread.interrupt();
    try {
      mJvmMonitorThread.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    reset();
  }

  /**
   * Resets value of mJvmMonitorThread.
   */
  public void reset() {
    mJvmMonitorThread = null;
  }

  /**
   * @return true if thread started,false otherwise
   */
  public boolean isStarted() {
    return mJvmMonitorThread != null;
  }

  /**
   * @return time exceeds WARN threshold in milliseconds
   */
  public long getWarnTimeExceeded() {
    return mWarnTimeExceeded;
  }

  /**
   * @return time exceeds INFO threshold in milliseconds
   */
  public long getInfoTimeExceeded() {
    return mInfoTimeExceeded;
  }

  /**
   * @return Total extra time in milliseconds
   */
  public long getTotalExtraTime() {
    return mTotalExtraTimeMs;
  }

  private String getMemoryInfo() {
    Runtime runtime = Runtime.getRuntime();
    return "max memory = " + runtime.maxMemory() / (1024 * 1024) + "M total memory = "
        + runtime.totalMemory() / (1024 * 1024) + "M free memory = "
        + runtime.freeMemory() / (1024 * 1024) + "M";
  }

  private String formatLogString(long extraSleepTime,
      Map<String, GarbageCollectorMXBean> gcMXBeanMapBeforeSleep,
        Map<String, GarbageCollectorMXBean> gcMXBeanMapAfterSleep) {
    List<String> beanDiffs = Lists.newArrayList();
    GarbageCollectorMXBean oldBean;
    GarbageCollectorMXBean newBean;
    Set<String> nameSet = Sets.intersection(gcMXBeanMapBeforeSleep.keySet(),
        gcMXBeanMapAfterSleep.keySet());
    for (String name : nameSet) {
      oldBean = gcMXBeanMapBeforeSleep.get(name);
      newBean = gcMXBeanMapAfterSleep.get(name);
      if (oldBean == null) {
        beanDiffs.add("new GCBean created name= '" + newBean.getName() + " count="
            + newBean.getCollectionCount() + " time=" + newBean.getCollectionTime() + "ms");
      } else if (newBean == null) {
        beanDiffs.add("old GCBean canceled name= '" + oldBean.getName() + " count="
            + oldBean.getCollectionCount() + " time=" + oldBean.getCollectionTime() + "ms");
      } else {
        if (oldBean.getCollectionTime() != newBean.getCollectionTime()
            || oldBean.getCollectionCount() != newBean.getCollectionCount()) {
          beanDiffs.add("GC name= '" + newBean.getName() + " count="
              + newBean.getCollectionCount() + " time=" + newBean.getCollectionTime() + "ms");
        }
      }
    }
    StringBuilder ret = new StringBuilder().append("JVM paused ").append(extraSleepTime)
        .append("ms\n");
    if (beanDiffs.isEmpty()) {
      ret.append("No GCs detected ");
    } else {
      ret.append("GC list:\n" + Joiner.on("\n").join(beanDiffs));
    }
    ret.append("\n").append(getMemoryInfo());
    return ret.toString();
  }

  private Map<String, GarbageCollectorMXBean> getGarbageCollectorMXBeans() {
    List<GarbageCollectorMXBean> gcBeanList = ManagementFactory.getGarbageCollectorMXBeans();
    Map<String, GarbageCollectorMXBean> gcBeanMap = new HashMap();
    for (GarbageCollectorMXBean gcBean : gcBeanList) {
      gcBeanMap.put(gcBean.getName(), gcBean);
    }
    return gcBeanMap;
  }

  private class GcMonitor extends Thread {

    public GcMonitor() {
      setDaemon(true);
    }

    @Override
    public void run() {
      Stopwatch sw = new Stopwatch();
      Map<String, GarbageCollectorMXBean> gcBeanMapBeforeSleep = getGarbageCollectorMXBeans();
      while (true) {
        sw.reset().start();
        try {
          Thread.sleep(mGcSleepIntervalMs);
        } catch (InterruptedException ie) {
          LOG.warn(ie.getStackTrace());
          return;
        }
        long extraTime = sw.elapsed(TimeUnit.MILLISECONDS) - mGcSleepIntervalMs;
        mTotalExtraTimeMs += extraTime;
        Map<String, GarbageCollectorMXBean> gcBeanMapAfterSleep = getGarbageCollectorMXBeans();

        if (extraTime > mWarnThresholdMs) {
          mInfoTimeExceeded++;
          mWarnTimeExceeded++;
          LOG.warn(formatLogString(extraTime, gcBeanMapBeforeSleep, gcBeanMapAfterSleep));
        } else if (extraTime > mInfoThresholdMs) {
          mInfoTimeExceeded++;
          LOG.info(formatLogString(
              extraTime, gcBeanMapBeforeSleep, gcBeanMapAfterSleep));
        }
        gcBeanMapBeforeSleep = gcBeanMapAfterSleep;
      }
    }
  }
}
