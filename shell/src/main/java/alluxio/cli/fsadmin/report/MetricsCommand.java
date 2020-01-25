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

import alluxio.client.meta.MetaMasterClient;
import alluxio.grpc.MetricValue;
import alluxio.metrics.MetricKey;
import alluxio.util.FormatUtils;

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
  private String mInfoFormat = "%-40s %20s";
  private Map<String, MetricValue> mMetricsMap;

  /**
   * Creates a new instance of {@link MetricsCommand}.
   *
   * @param metaMasterClient client to connect to meta master client
   * @param printStream stream to print operation metrics information to
   */
  public MetricsCommand(MetaMasterClient metaMasterClient, PrintStream printStream)
      throws IOException {
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
    Long bytesReadLocal = mMetricsMap.getOrDefault(MetricKey.CLUSTER_BYTES_READ_LOCAL.getName(),
        MetricValue.newBuilder().setLongValue(0L).build()).getLongValue();
    Long bytesReadRemote = mMetricsMap.getOrDefault(
        MetricKey.CLUSTER_BYTES_READ_ALLUXIO.getName(),
        MetricValue.newBuilder().setLongValue(0L).build()).getLongValue();
    Long bytesReadUfs = mMetricsMap.getOrDefault(
        MetricKey.CLUSTER_BYTES_READ_UFS_ALL.getName(),
        MetricValue.newBuilder().setLongValue(0L).build()).getLongValue();

    mPrintStream.println("Total IO: ");
    printMetric(MetricKey.CLUSTER_BYTES_READ_LOCAL.getName(),
        "Short-circuit Read", true);
    printMetric(MetricKey.CLUSTER_BYTES_READ_DOMAIN.getName(),
        "Short-circuit Read (Domain Socket)", true);
    printMetric(MetricKey.CLUSTER_BYTES_READ_ALLUXIO.getName(),
        "From Remote Instances", true);
    printMetric(MetricKey.CLUSTER_BYTES_READ_UFS_ALL.getName(),
        "Under Filesystem Read", true);
    printMetric(MetricKey.CLUSTER_BYTES_WRITTEN_ALLUXIO.getName(),
        "Alluxio Write", true);
    printMetric(MetricKey.CLUSTER_BYTES_WRITTEN_DOMAIN.getName(),
        "Alluxio Write (Domain Socket)", true);
    printMetric(MetricKey.CLUSTER_BYTES_WRITTEN_UFS_ALL.getName(),
        "Under Filesystem Write", true);

    mPrintStream.println("\nTotal IO Throughput (Last Minute): ");
    printMetric(MetricKey.CLUSTER_BYTES_READ_LOCAL_THROUGHPUT.getName(),
        "Short-circuit Read", true);
    printMetric(MetricKey.CLUSTER_BYTES_READ_DOMAIN_THROUGHPUT.getName(),
        "Short-circuit Read (Domain Socket)", true);
    printMetric(MetricKey.CLUSTER_BYTES_READ_ALLUXIO_THROUGHPUT.getName(),
        "From Remote Instances", true);
    printMetric(MetricKey.CLUSTER_BYTES_READ_UFS_THROUGHPUT.getName(),
        "Under Filesystem Read", true);
    printMetric(MetricKey.CLUSTER_BYTES_WRITTEN_ALLUXIO_THROUGHPUT.getName(),
        "Alluxio Write", true);
    printMetric(MetricKey.CLUSTER_BYTES_WRITTEN_DOMAIN_THROUGHPUT.getName(),
        "Alluxio Write (Domain Socket)", true);
    printMetric(MetricKey.CLUSTER_BYTES_WRITTEN_UFS_THROUGHPUT.getName(),
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
    printMetric(MetricKey.MASTER_DIRECTORIES_CREATED.getName(), "Directories Created", false);
    printMetric(MetricKey.MASTER_FILE_BLOCK_INFOS_GOT.getName(), "File Block Infos Got", false);
    printMetric(MetricKey.MASTER_FILE_INFOS_GOT.getName(), "File Infos Got", false);
    printMetric(MetricKey.MASTER_FILES_COMPLETED.getName(), "Files Completed", false);
    printMetric(MetricKey.MASTER_FILES_CREATED.getName(), "Files Created", false);
    printMetric(MetricKey.MASTER_FILES_FREED.getName(), "Files Freed", false);
    printMetric(MetricKey.MASTER_FILES_PERSISTED.getName(), "Files Persisted", false);
    printMetric(MetricKey.MASTER_NEW_BLOCKS_GOT.getName(), "New Blocks Got", false);
    printMetric(MetricKey.MASTER_PATHS_DELETED.getName(), "Paths Deleted", false);
    printMetric(MetricKey.MASTER_PATHS_MOUNTED.getName(), "Paths Mounted", false);
    printMetric(MetricKey.MASTER_PATHS_RENAMED.getName(), "Paths Renamed", false);
    printMetric(MetricKey.MASTER_PATHS_UNMOUNTED.getName(), "Paths Unmounted", false);

    mPrintStream.println("\nRPC Invocations: ");
    printMetric(MetricKey.MASTER_COMPLETE_FILE_OPS.getName(), "Complete File Operations", false);
    printMetric(MetricKey.MASTER_CREATE_DIRECTORIES_OPS.getName(),
        "Create Directory Operations", false);
    printMetric(MetricKey.MASTER_CREATE_FILES_OPS.getName(), "Create File Operations", false);
    printMetric(MetricKey.MASTER_DELETE_PATHS_OPS.getName(),
        "Delete Path Operations", false);
    printMetric(MetricKey.MASTER_FREE_FILE_OPS.getName(),
        "Free File Operations", false);
    printMetric(MetricKey.MASTER_GET_FILE_BLOCK_INFO_OPS.getName(),
        "Get File Block Info Operations", false);
    printMetric(MetricKey.MASTER_GET_FILE_INFO_OPS.getName(), "Get File Info Operations", false);
    printMetric(MetricKey.MASTER_GET_NEW_BLOCK_OPS.getName(), "Get New Block Operations", false);
    printMetric(MetricKey.MASTER_MOUNT_OPS.getName(), "Mount Operations", false);
    printMetric(MetricKey.MASTER_RENAME_PATH_OPS.getName(), "Rename Path Operations", false);
    printMetric(MetricKey.MASTER_SET_ACL_OPS.getName(), "Set ACL Operations", false);
    printMetric(MetricKey.MASTER_SET_ATTRIBUTE_OPS.getName(), "Set Attribute Operations", false);
    printMetric(MetricKey.MASTER_UNMOUNT_OPS.getName(), "Unmount Operations", false);

    // TODO(lu) improve printout info to sync with web UI
    mPrintStream.println("\nOther Metrics: ");
    mInfoFormat = "%s  (%s)"; // Some property names are too long to fit in previous info format
    for (Map.Entry<String, MetricValue> entry : mMetricsMap.entrySet()) {
      mPrintStream.println(INDENT + String.format(mInfoFormat,
          entry.getKey(), getFormattedValue(entry.getValue())));
    }
    return 0;
  }

  /**
   * Prints the metrics information.
   *
   * @param metricName the metric name to get a metric value
   * @param nickName the metric name to print
   * @param valueIsBytes whether the metric value is bytes
   */
  private void printMetric(String metricName, String nickName, boolean valueIsBytes) {
    if (mMetricsMap == null || !mMetricsMap.containsKey(metricName)) {
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
    if (metricValue.hasDoubleValue()) {
      return DECIMAL_FORMAT.format(metricValue.getDoubleValue());
    } else {
      return DECIMAL_FORMAT.format(metricValue.getLongValue());
    }
  }
}
