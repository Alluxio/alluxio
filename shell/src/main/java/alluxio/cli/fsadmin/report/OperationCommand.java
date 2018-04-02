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
import java.util.Map;
import java.util.TreeMap;

/**
 * Prints Alluxio operation metrics information.
 */
public class OperationCommand {
  private static final int INDENT_SIZE = 4;
  private static final String INFO_FORMAT = "%-30s %10s";

  private int mIndentationLevel = 0;
  private FileSystemMasterClient mFileSystemMasterClient;
  private PrintStream mPrintStream;

  /**
   * Creates a new instance of {@link OperationCommand}.
   *
   * @param fileSystemMasterClient client to connect to filesystem master client
   * @param printStream stream to print operation metrics information to
   */
  public OperationCommand(FileSystemMasterClient fileSystemMasterClient, PrintStream printStream) {
    mFileSystemMasterClient = fileSystemMasterClient;
    mPrintStream = printStream;
  }

  /**
   * Runs report operation command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    print("Alluxio logical operations: ");
    mIndentationLevel++;
    Map<String, Long> operationInfo = new TreeMap<>(mFileSystemMasterClient.getOperationInfo());
    for (Map.Entry<String, Long> entry : operationInfo.entrySet()) {
      String operationName = entry.getKey();
      if (!operationName.startsWith("UfsSessionCount-Ufs")) {
        // TODO(lu) add ufs session count info in report ufs command
        print(String.format(INFO_FORMAT, getReadableName(operationName), entry.getValue()));
      }
    }
    mIndentationLevel--;

    print("\nAlluxio RPC invocations: ");
    mIndentationLevel++;
    Map<String, Long> rpcInvocationInfo = new TreeMap<>(mFileSystemMasterClient.getRpcInvocationInfo());
    for (Map.Entry<String, Long> entry : rpcInvocationInfo.entrySet()) {
      print(String.format(INFO_FORMAT, getReadableName(entry.getKey()), entry.getValue()));
    }
    mIndentationLevel--;
    return 0;
  }

  /**
   * Transforms the key name to a readable name
   *
   * @param keyName the key name to transform
   * @return a readable name from the input key
   */
  private String getReadableName(String keyName) {
    return keyName.replaceAll("(.)([A-Z])", "$1 $2")
        .replaceAll("Ops", "Operations");
  }

  /**
   * Prints indented information.
   *
   * @param text information to print
   */
  private void print(String text) {
    String indent = Strings.repeat(" ", mIndentationLevel * INDENT_SIZE);
    mPrintStream.println(indent + text);
  }
}
