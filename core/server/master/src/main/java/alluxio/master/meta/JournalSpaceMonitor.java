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

package alluxio.master.meta;

import alluxio.AlluxioURI;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.shell.CommandReturn;
import alluxio.util.LogUtils;
import alluxio.util.ShellUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A heartbeat executor which periodically tracks the amount of disk space available for a
 * particular path, generally the disk holding the journal.
 *
 * This monitor is only applicable to systems which are running on the Linux OS and which are
 * running the embedded journal. Other configurations are invalid. Running this monitor on such
 * systems is undefined behavior.
 */
public class JournalSpaceMonitor implements HeartbeatExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(JournalSpaceMonitor.class);

  /**
   * Convenience method for {@link #JournalSpaceMonitor(String, long)}.
   *
   * @param configuration the current alluxio configuration
   */
  public JournalSpaceMonitor(AlluxioConfiguration configuration) {
    this(configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER), configuration.getLong(
        PropertyKey.MASTER_JOURNAL_SPACE_MONITOR_PERCENT_FREE_THRESHOLD));
  }

  private final String mJournalPath;
  private final long mWarnCapacityPercentThreshold;

  // Used to store information for metrics gauges
  private Map<String, DiskInfo> mMetricInfo = new HashMap<>();

  /**
   *
   * @param journalPath the path to the journal location
   * @param warnCapacityPercentThreshold when a device utilization falls below this threshold,
   *                                     emit warnings to the log
   */
  public JournalSpaceMonitor(String journalPath, long warnCapacityPercentThreshold) {
    Preconditions.checkArgument(Files.exists(Paths.get(journalPath)),
        "journal path must exist");
    mJournalPath = journalPath;
    mWarnCapacityPercentThreshold = warnCapacityPercentThreshold;
  }

  /**
   * Gets the raw output from the {@code df} command.
   *
   * This method is used to allow testing of the JournalSpaceMonitor code.
   *
   * @return the output from the {@code df} command
   * @throws IOException if retrieving the output fails
   */
  @VisibleForTesting
  CommandReturn getRawDiskInfo() throws IOException {
    return ShellUtils.execCommandWithOutput("df", "-k", "-P", mJournalPath);
  }

  /**
   * Gets the current disk information from the system for the journal path.
   *
   * @return a list of the disks that back the journal path. Many systems will just have one
   * disk. Depending on the configuration multiple disks may back the journal path, in which
   * case we report information on all of them
   * @throws IOException when obtaining the disk information fails
   */
  protected synchronized List<DiskInfo> getDiskInfo() throws IOException {
    // command allowed by the POSIX specification
    CommandReturn output = getRawDiskInfo();
    if (output.getExitCode() != 0) {
      throw new IOException("Command failed with exit code " + output.getExitCode());
    }
    List<DiskInfo> infos = Arrays.stream(output.getOutput().split("\n"))
        .skip(1)
        .map(infoLine -> Arrays.stream(infoLine.split(" "))
              .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList()))
        .filter(data -> data.size() >= 6)
        .map(data -> {
          String diskPath = data.get(0);
          // total size is not necessarily (used size + available size)
          long totalSize = Long.parseLong(data.get(1));
          long usedSize = Long.parseLong(data.get(2));
          long availableSize = Long.parseLong(data.get(3));
          String mountedFs = data.get(5);
          return new DiskInfo(diskPath, totalSize, usedSize, availableSize, mountedFs);
        }).collect(Collectors.toList());
    mMetricInfo = infos.stream().collect(Collectors.toMap(DiskInfo::getDiskPath, f -> f));
    infos.forEach(info -> {
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
              MetricKey.MASTER_JOURNAL_SPACE_FREE_BYTES.getName()
                  + "Device" + MetricsSystem.escape(new AlluxioURI(info.getDiskPath()))), () -> {
          DiskInfo metricInfo = mMetricInfo.get(info.getDiskPath());
          if (metricInfo != null) {
            return metricInfo.getAvailableBytes();
          } else {
            return -1;
          }
        });
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
          MetricKey.MASTER_JOURNAL_SPACE_FREE_PERCENT.getName()
              + "Device" + MetricsSystem.escape(new AlluxioURI(info.getDiskPath()))), () -> {
          DiskInfo metricInfo = mMetricInfo.get(info.getDiskPath());
          if (metricInfo != null) {
            return metricInfo.getPercentAvailable();
          } else {
            return -1;
          }
        });
    });
    return infos;
  }

  @Override
  public void heartbeat() throws InterruptedException {
    try {
      List<DiskInfo> currentInfo = getDiskInfo();
      currentInfo.forEach(info -> {
        if (info.getPercentAvailable() < mWarnCapacityPercentThreshold) {
          LOG.warn(String.format(
              "The journal disk %s backing the journal has only %.2f%% space left - %s",
              info.getDiskPath(), info.getPercentAvailable(), info));
        }
      });
    } catch (IOException e) {
      LogUtils.warnWithException(LOG, "Failed to get journal disk information", e);
    }
  }

  @Override
  public void close() {
    // noop
  }

  /**
   * A class representing the state of a physical device.
   */
  public static class DiskInfo {
    private final String mDiskPath;
    private final long mUsedBytes;
    private final long mTotalAllocatedBytes;
    private final long mAvailableBytes;
    private final double mPercentAvailable;
    private final String mMountPath;

    /**
     * Create a new instance of {@link DiskInfo} representing the current utilization for a
     * particular block device.
     *
     * @param diskPath the path to the raw device
     * @param totalAllocatedBytes the total filesystem
     * @param usedBytes the amount of bytes used by the filesystem
     * @param availableBytes the amount of bytes available on the filesystem
     * @param mountPath the path where the device is mounted
     */
    public DiskInfo(String diskPath, long totalAllocatedBytes, long usedBytes, long availableBytes,
        String mountPath) {
      mDiskPath = diskPath;
      mTotalAllocatedBytes = totalAllocatedBytes * 1024;
      mUsedBytes = usedBytes * 1024;
      mAvailableBytes = availableBytes * 1024;
      mMountPath = mountPath;
      mPercentAvailable = 100 * ((double) mAvailableBytes / (mAvailableBytes + mUsedBytes));
    }

    /**
     * @return the raw device path
     */
    public String getDiskPath() {
      return mDiskPath;
    }

    /**
     * @return the bytes used by the device
     */
    public long getUsedBytes() {
      return mUsedBytes;
    }

    /**
     * @return the total bytes allocated for the filesystem on this device
     */
    public long getTotalAllocatedBytes() {
      return mTotalAllocatedBytes;
    }

    /**
     * @return the remaining available bytes on this disk
     */
    public long getAvailableBytes() {
      return mAvailableBytes;
    }

    /**
     * @return the path where this disk is mounted
     */
    public String getMountPath() {
      return mMountPath;
    }

    /**
     * @return the percent of remaining space available on this disk
     */
    public double getPercentAvailable() {
      return mPercentAvailable;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("diskPath", mDiskPath)
          .add("totalAllocatedBytes", mTotalAllocatedBytes)
          .add("usedBytes", mUsedBytes)
          .add("availableBytes", mAvailableBytes)
          .add("percentAvailable", mPercentAvailable)
          .add("mountPath", mMountPath)
          .toString();
    }
  }
}
