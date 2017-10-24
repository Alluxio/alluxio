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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Class to monitor JVM with a daemon thread, the thread sleep period of time
 * and get the true time the sleep takes. If it is longer than it should be, the
 * JVM has paused processing. Then log it into different level.
 */
public final class JvmPauseMonitor {
  private static final Log LOG = LogFactory.getLog(
      JvmPauseMonitor.class);

  /** The time to sleep. */
  private final long mGcSleepIntervalMs;

  /** Extra sleep time longer than this threshold, log WARN. */
  private final long mWarnThresholdMs;

  /** Extra sleep time longer than this threshold, log INFO. */
  private final long mInfoThresholdMs;

  /** Times extra sleep time exceed WARN. */
  private long mWarnTimeExceededMS = 0;
  /** Times extra sleep time exceed INFO. */
  private long mInfoTimeExceededMS = 0;
  /** Total extra sleep time. */
  private long mTotalExtraTime = 0;

  private Thread mJvmMonitorThread;
  private volatile boolean mThreadStarted = true;

  /**
   * Constructs JvmPauseMonitor.
   */
  public JvmPauseMonitor() {
    mGcSleepIntervalMs = Configuration.getLong(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS);
    mWarnThresholdMs = Configuration.getLong(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS);
    mInfoThresholdMs = Configuration.getLong(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS);
  }

  /**
   * Starts jvm monitor thread.
   */
  public void start() {
    Preconditions.checkState(mJvmMonitorThread == null,
        "JVM monitor thread already started");
    mJvmMonitorThread = new Daemon(new Monitor());
    mJvmMonitorThread.start();
  }

  /**
   * Stops jvm monitor.
   */
  public void stop() {
    mThreadStarted = false;
    mJvmMonitorThread.interrupt();
    try {
      mJvmMonitorThread.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    reset();
  }

  /**
   * Resets value of mJvmMonitorThread and mThreadStarted.
   * @return reseted JvmPauseMonitor
   */
  public JvmPauseMonitor reset() {
    mJvmMonitorThread = null;
    mThreadStarted = true;
    return this;
  }

  /**
   * @return true if thread started,false otherwise
   */
  public boolean isStarted() {
    return mJvmMonitorThread != null;
  }

  /**
   * @return time exceeds WARN threshold
   */
  public long getWarnTimeExceeded() {
    return mWarnTimeExceededMS;
  }

  /**
   * @return time exceeds INFO threshold
   */
  public long getInfoTimeExceeded() {
    return mInfoTimeExceededMS;
  }

  /**
   * @return Total extra time
   */
  public long getTotalExtraTime() {
    return mTotalExtraTime;
  }

  private String getMemoryInfo() {
    Runtime runtime = Runtime.getRuntime();
    return "max memory = " + runtime.maxMemory() / (1024 * 1024) + "M total memory = "
        + runtime.totalMemory() / (1024 * 1024) + "M free memory = "
        + runtime.freeMemory() / (1024 * 1024) + "M";
  }

  private String formatLogString(long extraSleepTime,
      List<GarbageCollectorMXBean> gcMXBeanListBeforeSleep,
        List<GarbageCollectorMXBean> gcMXBeanListAfterSleep) {
    List<String> diffBean = Lists.newArrayList();
    GarbageCollectorMXBean oldBean;
    GarbageCollectorMXBean newBean;
    for (int i = 0; i < gcMXBeanListBeforeSleep.size(); i++) {
      oldBean = gcMXBeanListBeforeSleep.get(i);
      newBean = gcMXBeanListAfterSleep.get(i);
      if (oldBean.getCollectionTime() != newBean.getCollectionTime()
          || oldBean.getCollectionCount() != newBean.getCollectionCount()) {
        diffBean.add("GC name= '" + newBean.getName() + " count="
            + newBean.getCollectionCount() + " time=" + newBean.getCollectionTime() + "ms");
      }
    }
    String ret = "JVM pause " + extraSleepTime + "ms\n";
    if (diffBean.isEmpty()) {
      ret += "No GCs detected ";
    } else {
      ret += "GC list:\n" + Joiner.on("\n").join(diffBean);
    }
    ret += getMemoryInfo();
    return ret;
  }

  private List<GarbageCollectorMXBean> getGarbageCollectorMXBeanList() {
    return ManagementFactory.getGarbageCollectorMXBeans();
  }

  private class Monitor implements Runnable {
    @Override
    public void run() {
      Stopwatch sw = new Stopwatch();
      List<GarbageCollectorMXBean> gcBeanListBeforeSleep = getGarbageCollectorMXBeanList();
      while (mThreadStarted) {
        sw.reset().start();
        try {
          Thread.sleep(mGcSleepIntervalMs);
        } catch (InterruptedException ie) {
          LOG.warn(ie.getStackTrace());
          return;
        }
        long extraTime = sw.elapsed(TimeUnit.MILLISECONDS) - mGcSleepIntervalMs;
        mTotalExtraTime += extraTime;
        List<GarbageCollectorMXBean> gcBeanListAfterSleep = getGarbageCollectorMXBeanList();

        if (extraTime > mWarnThresholdMs) {
          ++mWarnTimeExceededMS;
          LOG.warn(formatLogString(
              extraTime, gcBeanListBeforeSleep, gcBeanListAfterSleep));
        } else if (extraTime > mInfoThresholdMs) {
          ++mInfoTimeExceededMS;
          LOG.info(formatLogString(
              extraTime, gcBeanListBeforeSleep, gcBeanListAfterSleep));
        }
        gcBeanListBeforeSleep = gcBeanListAfterSleep;
      }
    }
  }
}
