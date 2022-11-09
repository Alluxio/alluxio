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

import alluxio.client.block.BlockMasterClient;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Strings;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

/**
 * Prints Alluxio cluster worker information.
 */
public class WorkersCommand {
  private static final int INDENT_SIZE = 4;

  private int mIndentationLevel = 0;
  private BlockMasterClient mBlockMasterClient;
  private PrintStream mPrintStream;

  /**
   * Creates a new instance of {@link WorkersCommand}.
   *
   * @param blockMasterClient client to connect to block master
   * @param printStream stream to print summary information to
   */
  public WorkersCommand(BlockMasterClient blockMasterClient, PrintStream printStream) {
    mBlockMasterClient = blockMasterClient;
    mPrintStream = printStream;
  }

  /**
   * Runs report summary command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    print("Alluxio workers: ");
    printWorkerInfo();
    return 0;
  }

  /**
   * Prints Alluxio meta master information.
   */
  private void printWorkerInfo() throws IOException {
    mIndentationLevel++;
    List<WorkerInfo> workerInfoList = mBlockMasterClient.getWorkerInfoList();

    for (WorkerInfo workerInfo : workerInfoList) {
      print(workerInfo.toString());
    }
    mIndentationLevel--;
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
