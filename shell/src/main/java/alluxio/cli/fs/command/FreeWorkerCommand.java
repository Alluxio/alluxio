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

import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.cli.fs.FileSystemShellUtils;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.wire.WorkerInfo;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Frees all blocks of given worker(s) synchronously from Alluxio cluster.
 */

@PublicApi
public final class FreeWorkerCommand extends AbstractFileSystemCommand {

  private static final int DEFAULT_PARALLELISM = 1;

  private static final String DEFAULT_WORKER_NAME = "";

  /**
   *
   * Constructs a new instance to free the given worker(s) from Alluxio.
   *
   * @param fsContext fs command context
   */
  public FreeWorkerCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  public int run(CommandLine cl) throws AlluxioException, IOException {
//    String workerName = FileSystemShellUtils.getWorkerNameArg(cl, HOSTS_OPTION, DEFAULT_WORKER_NAME);
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
    return "freeWorker -h $workerName";
  }

  @Override
  public String getDescription() {
    return "Frees all the blocks synchronously of specific worker(s) in Alluxio."
            + " Specify -t to set a maximum wait time.";
  }

}
