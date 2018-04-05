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
import alluxio.wire.MetricValue;

import com.google.common.base.Strings;

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
      = new DecimalFormat("###,###", new DecimalFormatSymbols(Locale.US));
  private static final int INDENT_LEVEL = 1;
  private static final int INDENT_SIZE = 4;
  private static final String INFO_FORMAT = "%-30s %20s";

  private final MetaMasterClient mMetaMasterClient;
  private final PrintStream mPrintStream;

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
    operations.add("DirectoriesCreated");
    operations.add("FileBlockInfosGot");
    operations.add("FileInfosGot");
    operations.add("FilesCompleted");
    operations.add("FilesCreated");
    operations.add("FilesFreed");
    operations.add("FilesPersisted");
    operations.add("FilesPinned");
    operations.add("NewBlocksGot");
    operations.add("PathsDeleted");
    operations.add("PathsMounted");
    operations.add("PathsRenamed");
    operations.add("PathsUnmounted");

    mPrintStream.println("Alluxio logical operations: ");
    metricsMap.put("FilesPinned", metricsMap.get("master.FilesPinned"));
    for (Map.Entry<String, MetricValue> entry : metricsMap.entrySet()) {
      String key = entry.getKey();
      if (operations.contains(key)) {
        printIndentedMetrics(key, entry.getValue().getLongValue());
      }
    }

    Set<String> rpcInvocations = new HashSet<>();
    rpcInvocations.add("CompleteFileOps");
    rpcInvocations.add("CreateDirectoryOps");
    rpcInvocations.add("CreateFileOps");
    rpcInvocations.add("DeletePathOps");
    rpcInvocations.add("FreeFileOps");
    rpcInvocations.add("GetFileBlockInfoOps");
    rpcInvocations.add("GetFileInfoOps");
    rpcInvocations.add("GetNewBlockOps");
    rpcInvocations.add("MountOps");
    rpcInvocations.add("RenamePathOps");
    rpcInvocations.add("SetAttributeOps");
    rpcInvocations.add("UnmountOps");

    mPrintStream.println("\nAlluxio RPC invocations: ");
    for (Map.Entry<String, MetricValue> entry : metricsMap.entrySet()) {
      String key = entry.getKey();
      if (rpcInvocations.contains(key)) {
        printIndentedMetrics(key, entry.getValue().getLongValue());
      }
    }
    return 0;
  }

  /**
   * Prints indented metrics information.
   *
   * @param key the key of the metrics property to print
   * @param value the value of the metrics property to print
   */
  private void printIndentedMetrics(String key, Long value) {
    String readableName = key.replaceAll("(.)([A-Z])", "$1 $2")
        .replaceAll("Ops", "Operations");
    String indent = Strings.repeat(" ", INDENT_LEVEL * INDENT_SIZE);
    String metricsInfo = String.format(INFO_FORMAT, readableName, DECIMAL_FORMAT.format(value));
    mPrintStream.println(indent + metricsInfo);
  }
}
