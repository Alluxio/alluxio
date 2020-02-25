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
import alluxio.metrics.MetricsSystem;
import alluxio.util.FormatUtils;

import com.google.common.math.DoubleMath;

import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Prints Alluxio metrics information.
 */
public class MetricsCommand {
  private static final String BYTES_METRIC_IDENTIFIER = "Bytes";
  private static final String THROUGHPUT_METRIC_IDENTIFIER = "Throughput";
  private static final DecimalFormat DECIMAL_FORMAT
      = new DecimalFormat("###,###.#####", new DecimalFormatSymbols(Locale.US));
  private static final String INFO_FORMAT = "%s  (Type: %s, Value: %s)%n";

  private final MetricsMasterClient mMetricsMasterClient;
  private final PrintStream mPrintStream;
  private Map<String, MetricValue> mMetricsMap;

  /**
   * Creates a new instance of {@link MetricsCommand}.
   *
   * @param metricsMasterClient client to connect to metrics master client
   * @param printStream stream to print operation metrics information to
   */
  public MetricsCommand(MetricsMasterClient metricsMasterClient, PrintStream printStream) {
    mMetricsMasterClient = metricsMasterClient;
    mPrintStream = printStream;
  }

  /**
   * Runs report metrics command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    mMetricsMap = mMetricsMasterClient.getMetrics();
    SortedSet<String> names = new TreeSet<>(mMetricsMap.keySet());
    for (String name : names) {
      if (!isAlluxioMetric(name)) {
        continue;
      }
      MetricValue metricValue = mMetricsMap.get(name);
      String strValue;
      if (metricValue.hasStringValue()) {
        strValue = metricValue.getStringValue();
      } else {
        double doubleValue = metricValue.getDoubleValue();
        if (name.contains(BYTES_METRIC_IDENTIFIER)) {
          // Bytes long can be transformed to human-readable format
          strValue = FormatUtils.getSizeFromBytes((long) doubleValue);
          if (name.contains(THROUGHPUT_METRIC_IDENTIFIER)) {
            strValue = strValue + "/MIN";
          }
        } else if (DoubleMath.isMathematicalInteger(doubleValue)) {
          strValue = DECIMAL_FORMAT.format((long) doubleValue);
        } else {
          strValue = String.valueOf(doubleValue);
        }
      }
      mPrintStream.printf(INFO_FORMAT, name, metricValue.getMetricType(), strValue);
    }
    return 0;
  }

  /**
   * Checks if a metric is Alluxio metric.
   *
   * @param name name of the metrics to check
   * @return true if a metric is an Alluxio metric, false otherwise
   */
  private boolean isAlluxioMetric(String name) {
    for (MetricsSystem.InstanceType instance : MetricsSystem.InstanceType.values()) {
      if (name.startsWith(instance.toString())) {
        return true;
      }
    }
    return false;
  }
}
