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

import alluxio.Constants;

import com.google.common.annotations.VisibleForTesting;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class to monitor JVM with a daemon thread, the thread sleep period of time
 * and get the true time the sleep takes. If it is longer than it should be, the
 * JVM has paused processing. Then log it into different level.
 */
@NotThreadSafe
public class JvmPauseMonitor {
  private static final Log LOG = LogFactory.getLog(JvmPauseMonitor.class);

  /** The time to sleep. */
  private final long mGcSleepIntervalMs;

  /** Extra sleep time longer than this threshold, log WARN. */
  private final long mWarnThresholdMs;

  /** Extra sleep time longer than this threshold, log INFO. */
  private final long mInfoThresholdMs;

  /** Times extra sleep time exceed WARN. */
  private final AtomicLong mWarnTimeExceeded = new AtomicLong();
  /** Times extra sleep time exceed INFO. */
  private final AtomicLong mInfoTimeExceeded = new AtomicLong();
  /** Total extra sleep time. */
  private final AtomicLong mTotalExtraTimeMs = new AtomicLong();

  private Thread mJvmMonitorThread;

  /**
   * Constructs JvmPauseMonitor.
   * @param gcSleepIntervalMs The time in milliseconds to sleep for in checking for GC pauses
   * @param warnThresholdMs when gc pauses exceeds this time in milliseconds log WARN
   * @param infoThresholdMs when gc pauses exceeds this time in milliseconds log INFO
   */
  public JvmPauseMonitor(long gcSleepIntervalMs, long warnThresholdMs, long infoThresholdMs) {
    Preconditions.checkArgument(gcSleepIntervalMs > 0, "gc sleep interval must be > 0");
    Preconditions.checkArgument(warnThresholdMs > 0, "warn threshold must be > 0");
    Preconditions.checkArgument(infoThresholdMs > 0, "info threshold must be > 0");
    Preconditions.checkArgument(warnThresholdMs > infoThresholdMs,
        "gc warn threshold must be > gc info threshold");
    mGcSleepIntervalMs = gcSleepIntervalMs;
    mWarnThresholdMs = warnThresholdMs;
    mInfoThresholdMs = infoThresholdMs;
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
    if (mJvmMonitorThread == null) {
      return;
    }
    mJvmMonitorThread.interrupt();
    try {
      mJvmMonitorThread.join(5 * Constants.SECOND_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    mJvmMonitorThread = null;
  }

  /**
   * @return true if thread started, false otherwise
   */
  public boolean isStarted() {
    return mJvmMonitorThread != null && mJvmMonitorThread.isAlive();
  }

  /**
   * @return time exceeds WARN threshold in milliseconds
   */
  public long getWarnTimeExceeded() {
    return mWarnTimeExceeded.get();
  }

  /**
   * @return time exceeds INFO threshold in milliseconds
   */
  public long getInfoTimeExceeded() {
    return mInfoTimeExceeded.get();
  }

  /**
   * @return Total extra time in milliseconds
   */
  public long getTotalExtraTime() {
    return mTotalExtraTimeMs.get();
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
    Map<String, GarbageCollectorMXBean> gcBeanMap = new HashMap<>();
    for (GarbageCollectorMXBean gcBean : gcBeanList) {
      gcBeanMap.put(gcBean.getName(), gcBean);
    }
    return gcBeanMap;
  }

  /**
   * Sleep this thread for the configured interval. Used indirectly in order to test
   *
   * @param ms time to sleep in milliseconds
   * @throws InterruptedException when the thread's sleep is interrupted
   */
  @VisibleForTesting
  void sleepMillis(long ms) throws InterruptedException {
    Thread.sleep(ms);
  }

  private class GcMonitor extends Thread {

    public GcMonitor() {
      setDaemon(true);
    }

    @Override
    public void run() {
      Stopwatch sw = Stopwatch.createUnstarted();
      Map<String, GarbageCollectorMXBean> gcBeanMapBeforeSleep = getGarbageCollectorMXBeans();
      while (!Thread.currentThread().isInterrupted()) {
        sw.reset().start();
        try {
          sleepMillis(mGcSleepIntervalMs);
        } catch (InterruptedException ie) {
          LOG.warn("JVM pause monitor interrupted during sleep.");
          return;
        }
        long extraTime = sw.elapsed(TimeUnit.MILLISECONDS) - mGcSleepIntervalMs;
        mTotalExtraTimeMs.addAndGet(extraTime);
        Map<String, GarbageCollectorMXBean> gcBeanMapAfterSleep = getGarbageCollectorMXBeans();

        if (extraTime > mWarnThresholdMs) {
          mInfoTimeExceeded.incrementAndGet();
          mWarnTimeExceeded.incrementAndGet();
          LOG.warn(formatLogString(extraTime, gcBeanMapBeforeSleep, gcBeanMapAfterSleep));
        } else if (extraTime > mInfoThresholdMs) {
          mInfoTimeExceeded.incrementAndGet();
          LOG.info(formatLogString(
              extraTime, gcBeanMapBeforeSleep, gcBeanMapAfterSleep));
        }
        gcBeanMapBeforeSleep = gcBeanMapAfterSleep;
      }
    }
  }
}
