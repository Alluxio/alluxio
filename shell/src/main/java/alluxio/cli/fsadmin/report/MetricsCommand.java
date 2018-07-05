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

package alluxio.cli.fsadmin.report;

import alluxio.client.MetaMasterClient;
import alluxio.metrics.ClientMetrics;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.WorkerMetrics;
import alluxio.util.FormatUtils;
import alluxio.wire.MetricValue;

import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Prints Alluxio metrics information.
 */
public class MetricsCommand {
  private static final DecimalFormat DECIMAL_FORMAT
      = new DecimalFormat("###,###.#####", new DecimalFormatSymbols(Locale.US));
  private static final String INDENT = "    ";

  private final MetaMasterClient mMetaMasterClient;
  private final PrintStream mPrintStream;
  private String mInfoFormat = "%-30s %20s";
  private Map<String, MetricValue> mMetricsMap;

  /**
   * Creates a new instance of {@link MetricsCommand}.
   *
   * @param metaMasterClient client to connect to meta master client
   * @param printStream stream to print operation metrics information to
   */
  public MetricsCommand(MetaMasterClient metaMasterClient, PrintStream printStream) {
    mMetaMasterClient = metaMasterClient;
    mPrintStream = printStream;
  }

  /**
   * Runs report metrics command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    mMetricsMap = new TreeMap<>(mMetaMasterClient.getMetrics());
    Long bytesReadLocal = mMetricsMap.getOrDefault(MetricsSystem.getClusterMetricName(
        ClientMetrics.BYTES_READ_LOCAL), MetricValue.forLong(0L)).getLongValue();
    Long bytesReadRemote = mMetricsMap.getOrDefault(MetricsSystem.getClusterMetricName(
        WorkerMetrics.BYTES_READ_ALLUXIO), MetricValue.forLong(0L)).getLongValue();
    Long bytesReadUfs =  mMetricsMap.getOrDefault(MetricsSystem.getClusterMetricName(
        WorkerMetrics.BYTES_READ_UFS_ALL), MetricValue.forLong(0L)).getLongValue();

    mPrintStream.println("Total IO Size: ");
    printMetric(MetricsSystem.getClusterMetricName(ClientMetrics.BYTES_READ_LOCAL),
        "Short-circuit Read", true);
    printMetric(MetricsSystem.getClusterMetricName(ClientMetrics.BYTES_READ_LOCAL),
        "Short-circuit Read", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_ALLUXIO),
        "From Remote Instances", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_UFS_ALL),
        "Under Filesystem Read", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_ALLUXIO),
        "Alluxio Write", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_UFS_ALL),
        "Under Filesystem Write", true);

    mPrintStream.println("\nTotal IO Throughput (Last Minute): ");
    printMetric(MetricsSystem.getClusterMetricName(ClientMetrics.BYTES_READ_LOCAL_THROUGHPUT),
        "Short-circuit Read", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_ALLUXIO_THROUGHPUT),
        "From Remote Instances", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_UFS_THROUGHPUT),
        "Under Filesystem Read", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_ALLUXIO_THROUGHPUT),
        "Alluxio Write", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_UFS_THROUGHPUT),
        "Under Filesystem Write", true);

    mPrintStream.println("\nCache Hit Rate (Percentage): ");
    long bytesReadTotal = bytesReadLocal + bytesReadRemote + bytesReadUfs;
    String cacheHitLocalPercentage = String.format("%.2f",
        (bytesReadTotal > 0) ? (100D * bytesReadLocal / bytesReadTotal) : 0);
    String cacheHitRemotePercentage = String.format("%.2f",
        (bytesReadTotal > 0) ? (100D * bytesReadRemote / bytesReadTotal) : 0);
    String cacheMissPercentage = String.format("%.2f",
        (bytesReadTotal > 0) ? (100D * bytesReadUfs / bytesReadTotal) : 0);

    mPrintStream.println(INDENT
        + String.format(mInfoFormat, "Alluxio Local", cacheHitLocalPercentage));
    mPrintStream.println(INDENT
        + String.format(mInfoFormat, "Alluxio Remote", cacheHitRemotePercentage));
    mPrintStream.println(INDENT
        + String.format(mInfoFormat, "Miss", cacheMissPercentage));

    mPrintStream.println("\nLogical Operations: ");
    // TODO(lu) change the metric names to constants
    printMetric("DirectoriesCreated", "Directories Created", false);
    printMetric("FileBlockInfosGot", "File Block Infos Got", false);
    printMetric("FileInfosGot", "File Infos Got", false);
    printMetric("FilesCompleted", "Files Completed", false);
    printMetric("FilesFreed", "Files Freed", false);
    printMetric("FilesPersisted", "Files Persisted", false);
    printMetric("FilesPinned", "Files Pinned", false);
    printMetric("NewBlocksGot", "New Blocks Got", false);
    printMetric("PathsDeleted", "Paths Deleted", false);
    printMetric("PathsMounted", "Paths Mounted", false);
    printMetric("PathsRenamed", "Paths Renamed", false);
    printMetric("PathsUnmounted", "Paths Unmounted", false);

    mPrintStream.println("\nRPC Invocations: ");
    printMetric("CompleteFileOps", "Directories Created", false);
    printMetric("CreateDirectoryOps", "Create Directory Operations", false);
    printMetric("CreateFileOps", "Create File Operations", false);
    printMetric("DeletePathOps", "Delete Path Operations", false);
    printMetric("FreeFileOps", "Free File Operations", false);
    printMetric("GetFileBlockInfoOps", "Get File Block Info Operations", false);
    printMetric("GetFileInfoOps", "Get File Info Operations", false);
    printMetric("GetNewBlockOps", "Get New Block Operations", false);
    printMetric("MountOps", "Mount Operations", false);
    printMetric("RenamePathOps", "Rename Path Operations", false);
    printMetric("SetAttributeOps", "Set Attribute Operations", false);
    printMetric("UnmountOps", "Unmount Operations", false);

    mPrintStream.println("\nOther metrics information: ");
    mInfoFormat = "%s  (%s)"; // Some property names are too long to fit in previous info format
    for (Map.Entry<String, MetricValue> entry : mMetricsMap.entrySet()) {
      mPrintStream.println(INDENT + String.format(mInfoFormat,
          entry.getKey(), getFormattedValue(entry.getValue())));
    }
    return 0;
  }

  /**
   * Print the metrics information.
   *
   * @param metricName the metric name to get metric value
   * @param nickName the metric name to print
   * @param valueIsBytes whether the metric value is bytes
   */
  private void printMetric(String metricName, String nickName, boolean valueIsBytes) {
    if (!mMetricsMap.containsKey(metricName)) {
      return;
    }
    MetricValue metricValue = mMetricsMap.get(metricName);
    String formattedValue = valueIsBytes ? FormatUtils.getSizeFromBytes(metricValue.getLongValue())
        : getFormattedValue(metricValue);
    mPrintStream.println(INDENT + String.format(mInfoFormat,
        nickName == null ? metricName : nickName, formattedValue));
    mMetricsMap.remove(metricName);
  }

  /**
   * Gets the formatted metric value.
   *
   * @param metricValue the metricValue to transform
   * @return the formatted metric value
   */
  private String getFormattedValue(MetricValue metricValue) {
    Double doubleValue = metricValue.getDoubleValue();
    Long longValue = metricValue.getLongValue();
    return doubleValue == null ? DECIMAL_FORMAT.format(longValue) :
        DECIMAL_FORMAT.format(doubleValue);
  }
}
