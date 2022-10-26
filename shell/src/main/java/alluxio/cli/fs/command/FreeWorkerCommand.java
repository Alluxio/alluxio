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

import alluxio.exception.status.NotFoundException;
import alluxio.resource.CloseableResource;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;
import static java.util.stream.Collectors.toList;

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

    // 1. Get the decommissioned BlockWorkerInfo to build a BlockWorkerClient in the future.
    List<BlockWorkerInfo> totalWorkers;

    try (CloseableResource<BlockMasterClient> masterClientResource =
                 mFsContext.acquireBlockMasterClientResource()) {
       totalWorkers = masterClientResource.get().getWorkerInfoList().stream()
              .map(w -> new BlockWorkerInfo(w.getAddress(), w.getCapacityBytes(), w.getUsedBytes()))
              .collect(toList());
    }

    BlockWorkerInfo targetBlockWorkerInfo = null;

    // 1. Get the BlockWorkerInfo of target worker.
    for (BlockWorkerInfo worker : totalWorkers)
      if (worker.getNetAddress().getHost().equals(workerName))
        targetBlockWorkerInfo = worker;

    if (targetBlockWorkerInfo == null)  {
      System.out.println("Target worker is not found in Alluxio.");
      return -1;
    }

    // 2. Remove target worker metadata.
    // TODO(Tony Sun): Rename all of the function and comments, freeDecommissionedWorker -> RemoveDecommissionedWorker.
    try (CloseableResource<BlockMasterClient> blockMasterClient =
                 mFsContext.acquireBlockMasterClientResource()) {
      blockMasterClient.get().removeDecommissionedWorker(workerName);
    } catch (NotFoundException ne) {
      System.out.println(ne.getMessage() + " Target worker is not in decommissioned state.");
      return -1;
    }

    // 3. Free target worker.
    try (CloseableResource<BlockWorkerClient> blockWorkerClient =
                 mFsContext.acquireBlockWorkerClient(targetBlockWorkerInfo.getNetAddress())) {
      blockWorkerClient.get().freeWorker();
    } catch (Exception e) {
      System.out.println("These directories are failed to be freed: " + e.getMessage());
      return -1;
    }

    System.out.println("Target worker has been freed successfully.");
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
    return "freeWorker <worker host name>";
  }

  @Override
  public String getDescription() {
    return "Synchronously free all the blocks" +
            " and directories of specific worker in Alluxio.";
  }

}
