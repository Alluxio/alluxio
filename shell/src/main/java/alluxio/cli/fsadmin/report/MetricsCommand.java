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

import alluxio.client.file.FileSystemMasterClient;

import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.metrics.MetricsSystem;
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
  private static final int INDENT_SIZE = 4;
  private static final String INFO_FORMAT = "%-30s %20s";

  private int mIndentationLevel = 1;
  private FileSystemMasterClient mFileSystemMasterClient;
  private PrintStream mPrintStream;
  private StringBuilder mCachedRpcInvocation;

  /**
   * Creates a new instance of {@link MetricsCommand}.
   *
   * @param fileSystemMasterClient client to connect to filesystem master client
   * @param printStream stream to print operation metrics information to
   */
  public MetricsCommand(FileSystemMasterClient fileSystemMasterClient, PrintStream printStream) {
    mFileSystemMasterClient = fileSystemMasterClient;
    mPrintStream = printStream;
    mCachedRpcInvocation = new StringBuilder();
  }

  /**
   * Runs report metrics command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    Map<String, Long> metricsMap = new TreeMap<>(mFileSystemMasterClient.getMetrics());
    metricsMap.put("FilesPinned", metricsMap.get("master.FilesPinned"));

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

    mPrintStream.println("Alluxio logical operations: ");
    for (Map.Entry<String, Long> entry : metricsMap.entrySet()) {
      String key = entry.getKey();
      if (operations.contains(key)) {
        // Print operation info first
        printOrCache(key, entry.getValue(), false);
      } else if (rpcInvocations.contains(key)) {
        // Cache rpc invocation info and print later
        printOrCache(key, entry.getValue(), true);
      }
    }

    mPrintStream.println("\nAlluxio RPC invocations: ");
    mPrintStream.println(mCachedRpcInvocation.toString());
    return 0;
  }

  /**
   * Prints or caches indented metrics information of a certain metrics property.
   *
   * @param key the key of the metrics property
   * @param value the value of the metrics property
   * @param cache true to cache and print later, false to print directly
   */
  private void printOrCache(String key, Long value, boolean cache) {
    String readableName = key.replaceAll("(.)([A-Z])", "$1 $2")
        .replaceAll("Ops", "Operations");
    String indent = Strings.repeat(" ", mIndentationLevel * INDENT_SIZE);
    String metricsInfo = String.format(INFO_FORMAT,
        readableName, DECIMAL_FORMAT.format(value));
    if (cache) {
      mCachedRpcInvocation.append(indent).append(metricsInfo).append("\n");
    } else {
      mPrintStream.println(indent + metricsInfo);
    }
  }
}
