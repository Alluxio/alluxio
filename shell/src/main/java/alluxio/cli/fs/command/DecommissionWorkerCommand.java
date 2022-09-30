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
import alluxio.cli.fs.command.AbstractFileSystemCommand;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.grpc.Block;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockWorker;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import alluxio.grpc.FreeWorkerPOptions;
import alluxio.grpc.DecommissionWorkerPOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;


public final class DecommissionWorkerCommand extends AbstractFileSystemCommand{

  private static final int DEFAULT_TIMEOUT = 10 * Constants.MINUTE_MS;
  private static final Option TIMEOUT_OPTION =
          Option.builder("t")
                  .longOpt("timeout")
                  .argName("timeout in milliseconds")
                  .numberOfArgs(1)
                  .desc("Time in milliseconds for decommissioning a single worker to time out; default:"
                          + DEFAULT_TIMEOUT)
                  .required(false)
                  .build();

  private static final String DEFAULT_WORKER_NAME = "";

  private static final Option HOSTS_OPTION =
          Option.builder("h")
                  .longOpt("hosts")
                  .required(true)         // Host option is mandatory.
                  .hasArg(true)
                  .numberOfArgs(1)
                  .argName("hosts")
                  .desc("A worker host name, which is mandatory.")
                  .build();

  private static final Option FORCE_OPTION =
          Option.builder("f")
                  .required(false)
                  .hasArg(false)
                  .desc("force to decommission a worker.")
                  .build();

  public DecommissionWorkerCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  public int run(CommandLine cl) throws AlluxioException, IOException {
    int timeoutMS = (int)FileSystemShellUtils.getIntArg(cl, TIMEOUT_OPTION, DEFAULT_TIMEOUT);
    String workerName = FileSystemShellUtils.getWorkerNameArg(cl, HOSTS_OPTION, DEFAULT_WORKER_NAME);

    DecommissionWorkerPOptions options =
            DecommissionWorkerPOptions.newBuilder().setTimeOut(timeoutMS).setForced(cl.hasOption("f")).build();

    List<BlockWorkerInfo> cachedWorkers = mFsContext.getCachedWorkers();

    for (BlockWorkerInfo blockWorkerInfo :cachedWorkers)  {
      if (Objects.equals(blockWorkerInfo.getNetAddress().getHost(), workerName))  {
        mFileSystem.decommissionWorker(blockWorkerInfo.getNetAddress(), options);
        return 0;
      }
    }

    System.out.println("Target worker is not found in Alluxio, please input another name.");
    return 0;
  }

  @Override
  public String getCommandName() {
    return "decommissionWorker";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(TIMEOUT_OPTION)
            .addOption(HOSTS_OPTION)
            .addOption(FORCE_OPTION);
  }

  public String getUsage() {
    return "freeWorker [-t max_wait_time] <List<worker>>";
  }

  @Override
  public String getDescription() {
    return "Frees all the blocks synchronously of specific worker(s) in Alluxio."
            + " Specify -t to set a maximum wait time.";
  }
}
