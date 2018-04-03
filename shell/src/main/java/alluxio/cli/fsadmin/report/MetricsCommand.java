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

import com.google.common.base.Strings;

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
      = new DecimalFormat("###,###", new DecimalFormatSymbols(Locale.US));
  private static final int INDENT_SIZE = 4;
  private static final String INFO_FORMAT = "%-30s %20s";

  private int mIndentationLevel = 1;
  private FileSystemMasterClient mFileSystemMasterClient;
  private PrintStream mPrintStream;
  private Map<String, Long> mMetricsMap;

  /**
   * Creates a new instance of {@link MetricsCommand}.
   *
   * @param fileSystemMasterClient client to connect to filesystem master client
   * @param printStream stream to print operation metrics information to
   */
  public MetricsCommand(FileSystemMasterClient fileSystemMasterClient, PrintStream printStream) {
    mFileSystemMasterClient = fileSystemMasterClient;
    mPrintStream = printStream;
  }

  /**
   * Runs report metrics command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    mMetricsMap = mFileSystemMasterClient.getMetrics();

    mPrintStream.println("Alluxio logical operations: ");
    printMetricsInfo("DirectoriesCreated");
    printMetricsInfo("FileBlockInfosGot");
    printMetricsInfo("FileInfosGot");
    printMetricsInfo("FilesCompleted");
    printMetricsInfo("FilesCreated");
    printMetricsInfo("FilesFreed");
    printMetricsInfo("FilesPersisted");
    printMetricsInfo("FilesPinned");
    printMetricsInfo("NewBlocksGot");
    printMetricsInfo("PathsDeleted");
    printMetricsInfo("PathsMounted");
    printMetricsInfo("PathsRenamed");
    printMetricsInfo("PathsUnmounted");

    mPrintStream.println("\nAlluxio RPC invocations: ");
    printMetricsInfo("CompleteFileOps");
    printMetricsInfo("CreateDirectoryOps");
    printMetricsInfo("CreateFileOps");
    printMetricsInfo("DeletePathOps");
    printMetricsInfo("FreeFileOps");
    printMetricsInfo("GetFileBlockInfoOps");
    printMetricsInfo("GetFileInfoOps");
    printMetricsInfo("GetNewBlockOps");
    printMetricsInfo("MountOps");
    printMetricsInfo("RenamePathOps");
    printMetricsInfo("SetAttributeOps");
    printMetricsInfo("UnmountOps");
    return 0;
  }

  /**
   * Prints indented metrics information of a certain metrics property.
   *
   * @param key the key of the metrics property to print info of
   */
  private void printMetricsInfo(String key) {
    String readableName = key.replaceAll("(.)([A-Z])", "$1 $2")
        .replaceAll("Ops", "Operations");
    String indent = Strings.repeat(" ", mIndentationLevel * INDENT_SIZE);
    String metricsInfo = String.format(INFO_FORMAT,
        readableName, DECIMAL_FORMAT.format(mMetricsMap.get(key)));
    mPrintStream.println(indent + metricsInfo);
  }
}
