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

import alluxio.client.metrics.MetricsMasterClient;
import alluxio.grpc.MetricValue;
import alluxio.metrics.ClientMetrics;
import alluxio.metrics.MasterMetrics;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.WorkerMetrics;
import alluxio.util.FormatUtils;

import com.google.common.math.DoubleMath;

import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Prints Alluxio metrics information.
 */
public class MetricsCommand {
  private static final DecimalFormat DECIMAL_FORMAT
      = new DecimalFormat("###,###.#####", new DecimalFormatSymbols(Locale.US));
  private static final String INDENT = "    ";

  private final MetricsMasterClient mMetricsMasterClient;
  private final PrintStream mPrintStream;
  private String mInfoFormat = "%-40s %20s";
  private Map<String, MetricValue> mMetricsMap;

  /**
   * Creates a new instance of {@link MetricsCommand}.
   *
   * @param metricsMasterClient client to connect to metrics master client
   * @param printStream stream to print operation metrics information to
   */
  public MetricsCommand(MetricsMasterClient metricsMasterClient, PrintStream printStream)
      throws IOException {
    mMetricsMasterClient = metricsMasterClient;
    mPrintStream = printStream;
  }

  /**
   * Runs report metrics command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    Map<String, MetricValue> metrics = mMetricsMasterClient.getMetrics();
    // The returned map is unmodifiable so we make a copy of it
    mMetricsMap = new HashMap<>(metrics);

    MetricValue bytesReadLocalValue =
        mMetricsMap.get(MetricsSystem.getClusterMetricName(ClientMetrics.BYTES_READ_LOCAL));
    long bytesReadLocal = bytesReadLocalValue == null ? 0L
        : (long) bytesReadLocalValue.getDoubleValue();
    MetricValue bytesReadRemoteValue =
        mMetricsMap.get(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_ALLUXIO));
    long bytesReadRemote = bytesReadRemoteValue == null ? 0L
        : (long) bytesReadRemoteValue.getDoubleValue();

    MetricValue bytesReadUfsValue =
        mMetricsMap.get(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_UFS_ALL));
    long bytesReadUfs = bytesReadUfsValue == null ? 0L : (long) bytesReadUfsValue.getDoubleValue();

    mPrintStream.println("Total IO: ");
    printMetric(MetricsSystem.getClusterMetricName(ClientMetrics.BYTES_READ_LOCAL),
        "Short-circuit Read", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_DOMAIN),
        "Short-circuit Read (Domain Socket)", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_ALLUXIO),
        "From Remote Instances", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_UFS_ALL),
        "Under Filesystem Read", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_ALLUXIO),
        "Alluxio Write", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_DOMAIN),
        "Alluxio Write (Domain Socket)", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_UFS_ALL),
        "Under Filesystem Write", true);

    mPrintStream.println("\nTotal IO Throughput (Last Minute): ");
    printMetric(MetricsSystem.getClusterMetricName(ClientMetrics.BYTES_READ_LOCAL_THROUGHPUT),
        "Short-circuit Read", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_DOMAIN_THROUGHPUT),
        "Short-circuit Read (Domain Socket)", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_ALLUXIO_THROUGHPUT),
        "From Remote Instances", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_UFS_THROUGHPUT),
        "Under Filesystem Read", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_ALLUXIO_THROUGHPUT),
        "Alluxio Write", true);
    printMetric(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_DOMAIN_THROUGHPUT),
        "Alluxio Write (Domain Socket)", true);
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
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.DIRECTORIES_CREATED),
        "Directories Created", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.FILE_BLOCK_INFOS_GOT),
        "File Block Infos Got", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.FILE_INFOS_GOT),
        "File Infos Got", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.FILES_COMPLETED),
        "Files Completed", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.FILES_CREATED),
        "Files Created", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.FILES_FREED),
        "Files Freed", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.FILES_PERSISTED),
        "Files Persisted", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.NEW_BLOCKS_GOT),
        "New Blocks Got", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.PATHS_DELETED),
        "Paths Deleted", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.PATHS_MOUNTED),
        "Paths Mounted", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.PATHS_RENAMED),
        "Paths Renamed", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.PATHS_UNMOUNTED),
        "Paths Unmounted", false);

    mPrintStream.println("\nRPC Invocations: ");
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.COMPLETE_FILE_OPS),
        "Complete File Operations", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.CREATE_DIRECTORIES_OPS),
        "Create Directory Operations", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.CREATE_FILES_OPS),
        "Create File Operations", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.DELETE_PATHS_OPS),
        "Delete Path Operations", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.FREE_FILE_OPS),
        "Free File Operations", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.GET_FILE_BLOCK_INFO_OPS),
        "Get File Block Info Operations", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.GET_FILE_INFO_OPS),
        "Get File Info Operations", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.GET_NEW_BLOCK_OPS),
        "Get New Block Operations", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.MOUNT_OPS),
        "Mount Operations", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.RENAME_PATH_OPS),
        "Rename Path Operations", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.SET_ACL_OPS),
        "Set ACL Operations", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.SET_ATTRIBUTE_OPS),
        "Set Attribute Operations", false);
    printMetric(MetricsSystem.getMasterMetricName(MasterMetrics.UNMOUNT_OPS),
        "Unmount Operations", false);

    // TODO(lu) improve printout info to sync with web UI
    mPrintStream.println("\nOther Metrics: ");
    // Some property names are too long to fit in previous info format
    mInfoFormat = "%s  (Type: %s, Value: %s)";
    for (Map.Entry<String, MetricValue> entry : mMetricsMap.entrySet()) {
      String value = entry.getValue().hasStringValue() ? entry.getValue().getStringValue() :
          getFormattedDoubleValue(entry.getValue().getDoubleValue());
      mPrintStream.println(INDENT + String.format(mInfoFormat,
          entry.getKey(), entry.getValue().getMetricType(), value));
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
    MetricValue value = mMetricsMap.get(metricName);
    String formattedValue = value.hasStringValue() ? value.getStringValue()
        : valueIsBytes ? FormatUtils.getSizeFromBytes((long) value.getDoubleValue())
        : getFormattedDoubleValue(value.getDoubleValue());

    mPrintStream.println(INDENT + String.format(mInfoFormat,
        nickName == null ? metricName : nickName, formattedValue));
    mMetricsMap.remove(metricName);
  }

  private String getFormattedDoubleValue(double value) {
    if (DoubleMath.isMathematicalInteger(value)) {
      return DECIMAL_FORMAT.format((long) value);
    } else {
      return String.valueOf(value);
    }
  }
}
