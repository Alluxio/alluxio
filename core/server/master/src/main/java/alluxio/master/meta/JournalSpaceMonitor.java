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
import alluxio.wire.JournalDiskInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
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
  private AtomicReference<Map<String, JournalDiskInfo>> mMetricInfo = new AtomicReference();

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
  protected synchronized List<JournalDiskInfo> getDiskInfo() throws IOException {
    // command allowed by the POSIX specification
    CommandReturn output = getRawDiskInfo();
    if (output.getExitCode() != 0) {
      throw new IOException("Command failed with exit code " + output.getExitCode());
    }
    List<JournalDiskInfo> infos = Arrays.stream(output.getOutput().split("\n"))
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
          return new JournalDiskInfo(diskPath, totalSize, usedSize, availableSize, mountedFs);
        }).collect(Collectors.toList());
    mMetricInfo.set(infos.stream().collect(Collectors.toMap(JournalDiskInfo::getDiskPath, f -> f)));
    infos.forEach(info -> {
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
              MetricKey.MASTER_JOURNAL_SPACE_FREE_BYTES.getName()
                  + "Device" + MetricsSystem.escape(new AlluxioURI(info.getDiskPath()))), () -> {
          JournalDiskInfo metricInfo = mMetricInfo.get().get(info.getDiskPath());
          if (metricInfo != null) {
            return metricInfo.getAvailableBytes();
          } else {
            return -1;
          }
        });
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
          MetricKey.MASTER_JOURNAL_SPACE_FREE_PERCENT.getName()
              + "Device" + MetricsSystem.escape(new AlluxioURI(info.getDiskPath()))), () -> {
          JournalDiskInfo metricInfo = mMetricInfo.get().get(info.getDiskPath());
          if (metricInfo != null) {
            return metricInfo.getPercentAvailable();
          } else {
            return -1;
          }
        });
    });
    return infos;
  }

  /**
   * @return a list warning messages that can be logged or displayed to the user in the UI
   */
  public List<String> getJournalDiskWarnings() {
    try {
      List<JournalDiskInfo> currentInfo = getDiskInfo();
      return currentInfo.stream()
          .filter(info -> info.getPercentAvailable() < mWarnCapacityPercentThreshold)
          .map(info -> String.format(
                  "The journal disk %s backing the journal has only %.2f%% space left - %s",
              info.getDiskPath(), info.getPercentAvailable(), info))
          .collect(Collectors.toList());
    } catch (IOException e) {
      LogUtils.warnWithException(LOG, "Failed to get journal disk information. Critical warnings "
          + "about journal disk space may not appear in the logs.", e);
    }
    return Collections.emptyList();
  }

  @Override
  public void heartbeat() throws InterruptedException {
    getJournalDiskWarnings().forEach(LOG::warn);
  }

  @Override
  public void close() {
    // noop
  }
}
