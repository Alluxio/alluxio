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

package alluxio.cli.fs.command;

import alluxio.annotation.PublicApi;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;

import alluxio.grpc.FreeDecommissionedWorkerPOptions;
import alluxio.resource.CloseableResource;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Synchronously Frees all blocks of given worker from Alluxio cluster.
 */

@PublicApi
public final class FreeWorkerCommand extends AbstractFileSystemCommand {

  /**
   *
   * Constructs a new instance to free the given worker from Alluxio.
   *
   * @param fsContext fs command context
   */
  public FreeWorkerCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    String workerName = args[0];

    // 1. Get the decommissioned workerInfoList to build a BlockWorkerClient in the future.
    List<BlockWorkerInfo> decommissionedWorkers = mFsContext.getDecommissionedWorkers();

    // If decommissioned workers exist.
    if (!decommissionedWorkers.isEmpty())  {
      for (BlockWorkerInfo worker : decommissionedWorkers) {
        // If target worker is in the decommissioned worker list.
        if (Objects.equals(worker.getNetAddress().getHost(), workerName)) {
          // 2. Delete the metadata in master.
          try (CloseableResource<BlockMasterClient> blockMasterClient =
                  mFsContext.acquireBlockMasterClientResource()) {
            // TODO(Tony Sun): Need to add a if-else here? For the boolean return value.
            blockMasterClient.get().freeDecommissionedWorker(FreeDecommissionedWorkerPOptions
                    .newBuilder().setWorkerName(workerName).build());
          }
          // 3. freeWorker
          try (CloseableResource<BlockWorkerClient> blockWorkerClient =
                  mFsContext.acquireBlockWorkerClient(worker.getNetAddress())) {
            blockWorkerClient.get().freeWorker();
          } catch (Exception e) {
            System.out.println(e.getMessage());
          }
          return 0;
        }
      }
      System.out.println("Target worker is not found in Alluxio, " +
              "or is not decommissioned. Please input another worker name.");
    }
    else
      System.out.println("There are no decommissioned workers in cluster.");
    return 0;
  }

  @Override
  public String getCommandName() {
    return "freeWorker";
  }

  @Override
  public Options getOptions() {
    return new Options();
  }

  public String getUsage() {
    return "freeWorker $workerName";
  }

  @Override
  public String getDescription() {
    return "Synchronously Frees all the blocks of specific worker(s) in Alluxio.";
  }

}
