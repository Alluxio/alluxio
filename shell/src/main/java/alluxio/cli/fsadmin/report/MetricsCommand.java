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
import alluxio.metrics.MasterMetrics;
import alluxio.wire.MetricValue;

import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
  private String mInfoFormat = "%-25s %20s";

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
    Map<String, MetricValue> metricsMap = new TreeMap<>(mMetaMasterClient.getMetrics());
    Set<String> operations = new HashSet<>();
    operations.add(MasterMetrics.DIRECTORIES_CREATED);
    operations.add(MasterMetrics.FILE_BLOCK_INFOS_GOT);
    operations.add(MasterMetrics.FILE_INFOS_GOT);
    operations.add(MasterMetrics.FILES_COMPLETED);
    operations.add(MasterMetrics.FILES_CREATED);
    operations.add(MasterMetrics.FILES_FREED);
    operations.add(MasterMetrics.FILES_PERSISTED);
    operations.add(MasterMetrics.NEW_BLOCKS_GOT);
    operations.add(MasterMetrics.PATHS_DELETED);
    operations.add(MasterMetrics.PATHS_MOUNTED);
    operations.add(MasterMetrics.PATHS_RENAMED);
    operations.add(MasterMetrics.PATHS_UNMOUNTED);

    mPrintStream.println("Alluxio logical operations: ");
    for (Map.Entry<String, MetricValue> entry : metricsMap.entrySet()) {
      String key = entry.getKey();
      if (operations.contains(key)) {
        printIndentedMetrics(key, entry.getValue());
      }
    }

    Set<String> rpcInvocations = new HashSet<>();
    rpcInvocations.add(MasterMetrics.COMPLETE_FILE_OPS);
    rpcInvocations.add(MasterMetrics.CREATE_DIRECTORIES_OPS);
    rpcInvocations.add(MasterMetrics.CREATE_FILES_OPS);
    rpcInvocations.add(MasterMetrics.DELETE_PATHS_OPS);
    rpcInvocations.add(MasterMetrics.FREE_FILE_OPS);
    rpcInvocations.add(MasterMetrics.GET_FILE_BLOCK_INFO_OPS);
    rpcInvocations.add(MasterMetrics.GET_FILE_INFO_OPS);
    rpcInvocations.add(MasterMetrics.GET_NEW_BLOCK_OPS);
    rpcInvocations.add(MasterMetrics.MOUNT_OPS);
    rpcInvocations.add(MasterMetrics.RENAME_PATH_OPS);
    rpcInvocations.add(MasterMetrics.SET_ATTRIBUTE_OPS);
    rpcInvocations.add(MasterMetrics.UNMOUNT_OPS);

    mPrintStream.println("\nAlluxio RPC invocations: ");
    for (Map.Entry<String, MetricValue> entry : metricsMap.entrySet()) {
      String key = entry.getKey();
      if (rpcInvocations.contains(key)) {
        printIndentedMetrics(key, entry.getValue());
      }
    }

    mPrintStream.println("\nOther metrics information: ");
    mInfoFormat = "%s  (%s)"; // Some property names are too long to fit in previous info format
    for (Map.Entry<String, MetricValue> entry : metricsMap.entrySet()) {
      String key = entry.getKey();
      if (!operations.contains(key) && !rpcInvocations.contains(key)) {
        printIndentedMetrics(key, entry.getValue());
      }
    }
    return 0;
  }

  /**
   * Prints indented metrics information.
   *
   * @param name the property name to print
   * @param metricValue the metric value to print
   */
  private void printIndentedMetrics(String name, MetricValue metricValue) {
    Double doubleValue = metricValue.getDoubleValue();
    Long longValue = metricValue.getLongValue();

    String metricsInfo = String.format(mInfoFormat, name,
        doubleValue == null ? DECIMAL_FORMAT.format(longValue) :
            DECIMAL_FORMAT.format(doubleValue));

    mPrintStream.println(INDENT + metricsInfo);
  }
}
