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
import alluxio.grpc.StorageList;
import alluxio.grpc.WorkerLostStorageInfo;

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
    List<WorkerLostStorageInfo> workerLostStorageList = mBlockMasterClient.getWorkerLostStorage();
    if (workerLostStorageList.size() == 0) {
      mPrintStream.println("All worker storage paths are in working state.");
      return 0;
    }
    for (WorkerLostStorageInfo info : workerLostStorageList) {
      Map<String, StorageList> lostStorageMap = info.getLostStorageMap();
      if (lostStorageMap.size() != 0) {
        mPrintStream.printf("The following storage paths are lost in worker %s: %n",
            info.getAddress().getHost());
        for (Map.Entry<String, StorageList> tierStorage : lostStorageMap.entrySet()) {
          for (String storage : tierStorage.getValue().getStorageList()) {
            mPrintStream.printf("%s (%s)%n", storage, tierStorage.getKey());
          }
        }
      }
    }
    return 0;
  }
}
