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
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.wire.WorkerInfo;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;

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

    WorkerInfo workerInfo = mFsContext.getAndSetDecommissionStatusInMaster(workerName);
    if (workerInfo != null)  {
      mFileSystem.freeWorker(workerInfo.getAddress());
      System.out.println("Target worker is freed.");
      return 0;
    }
    else {
      System.out.println("Target worker is not found in Alluxio, " +
              "or is not be decommissioned. Please input another name.");
    }
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
