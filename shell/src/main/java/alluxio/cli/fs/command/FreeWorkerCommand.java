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
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;

import alluxio.exception.status.NotFoundException;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.resource.CloseableResource;
import io.grpc.StatusRuntimeException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;
import static java.util.stream.Collectors.toList;

/**
 * Synchronously free all blocks and directories of specific worker in Alluxio.
 */

@PublicApi
public final class FreeWorkerCommand extends AbstractFileSystemCommand {

  /**
   *
   * Constructs a new instance to free the given worker.
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
    List<WorkerNetAddress> totalWorkers;

    try (CloseableResource<BlockMasterClient> masterClientResource =
                 mFsContext.acquireBlockMasterClientResource()) {
       totalWorkers = masterClientResource.get().getWorkerInfoList().stream()
              .map(WorkerInfo::getAddress)
              .collect(toList());
    }

    WorkerNetAddress targetWorkerNetAddress = null;

    // 2. Get the BlockWorkerInfo of target worker.
    for (WorkerNetAddress workerNetAddress : totalWorkers) {
      if (workerNetAddress.getHost().equals(workerName))  {
        targetWorkerNetAddress = workerNetAddress;
        break;
      }
    }
    if (targetWorkerNetAddress == null)  {
      System.out.println("Worker " + workerName + " is not found in Alluxio.");
      return -1;
    }

    // 3. Remove target worker metadata.
    try (CloseableResource<BlockMasterClient> blockMasterClient =
                 mFsContext.acquireBlockMasterClientResource()) {
      blockMasterClient.get().removeDecommissionedWorker(workerName);
    } catch (NotFoundException notFoundException) {
      System.out.println("Worker" + workerName + " is not found in decommissioned worker set.");
      return -1;
    }

    // 4. Free target worker.
    try (CloseableResource<BlockWorkerClient> blockWorkerClient =
                 mFsContext.acquireBlockWorkerClient(targetWorkerNetAddress)) {
      blockWorkerClient.get().freeWorker();
    } catch (StatusRuntimeException statusRuntimeException) {
      System.out.println("Exception: " + statusRuntimeException.getMessage());
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
    return "Synchronously free all blocks" +
            " and directories of specific worker in Alluxio.";
  }

}
