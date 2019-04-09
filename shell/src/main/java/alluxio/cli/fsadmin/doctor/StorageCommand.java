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

package alluxio.cli.fsadmin.doctor;

import alluxio.client.block.BlockMasterClient;
import alluxio.wire.WorkerLostStorageInfo;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

/**
 * Prints server-side storage errors and warnings.
 */
public class StorageCommand {
  private final BlockMasterClient mBlockMasterClient;
  private final PrintStream mPrintStream;

  /**
   * Creates a new instance of {@link StorageCommand}.
   *
   * @param blockMasterClient client to get server-side worker lost storage information
   * @param printStream stream to print storage warnings to
   */
  public StorageCommand(BlockMasterClient blockMasterClient, PrintStream printStream) {
    mBlockMasterClient = blockMasterClient;
    mPrintStream = printStream;
  }

  /**
   * Runs doctor storage command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    List<WorkerLostStorageInfo> workerLostStorageInfo = mBlockMasterClient.getWorkerLostStorage();
    for (WorkerLostStorageInfo info : workerLostStorageInfo) {
      Map<String, List<String>> lostStorageOnTiers = info.getLostStorageOnTiers();
      if (lostStorageOnTiers.size() != 0) {
        mPrintStream.printf("The following directories are lost in worker %s: %n",
            info.getWorkerAddress().getHost());
        for (Map.Entry<String, List<String>> tierStorage : lostStorageOnTiers.entrySet()) {
          String tier = tierStorage.getKey();
          for (String storage : tierStorage.getValue()) {
            mPrintStream.printf("Tier %s: %s%n", tier, storage);
          }
        }
      }
    }
    return 0;
  }
}
