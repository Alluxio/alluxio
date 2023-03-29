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
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.grpc.DecommissionWorkerPOptions;
import alluxio.resource.CloseableResource;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Decommission a specific worker, the decommissioned worker is not automatically
 * shutdown and are not chosen for writing new replicas.
 */
public final class DecommissionWorkerCommand extends AbstractFileSystemCommand {

  private static final int DEFAULT_TIMEOUT = 10 * Constants.MINUTE_MS;

  private static final Option HOST_OPTION =
      Option.builder("h")
          .longOpt("host")
          .required(true)  // Host option is mandatory.
          .hasArg(true)
          .numberOfArgs(1)
          .argName("host")
          .desc("A worker host name, which is mandatory.")
          .build();

  /**
   * Constructs a new instance to decommission the given worker from Alluxio.
   * @param fsContext the filesystem of Alluxio
   */
  public DecommissionWorkerCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String workerHost = cl.getOptionValue(HOST_OPTION.getLongOpt());

    DecommissionWorkerPOptions options =
            DecommissionWorkerPOptions.newBuilder()
                .setWorkerName(workerHost).build();

    List<BlockWorkerInfo> cachedWorkers = mFsContext.getCachedWorkers();

    for (BlockWorkerInfo blockWorkerInfo : cachedWorkers)  {
      if (Objects.equals(blockWorkerInfo.getNetAddress().getHost(), workerHost))  {
        try (CloseableResource<BlockMasterClient> blockMasterClient =
                 mFsContext.acquireBlockMasterClientResource()) {
          long start = System.currentTimeMillis();
          blockMasterClient.get().decommissionWorker(options);
          long duration = System.currentTimeMillis() - start;
          System.out.printf("Decommission worker %s success, spend: %dms%n",
              workerHost, duration);
        } catch (IOException ie) {
          throw new AlluxioException(ie.getMessage());
        }
        return 0;
      }
    }

    System.out.println("Target worker is not found in Alluxio, please input another hostname.\n"
        + "Available workers:");
    for (BlockWorkerInfo blockWorkerInfo : cachedWorkers) {
      System.out.println("\t" + blockWorkerInfo.getNetAddress().getHost()
          + ":" + blockWorkerInfo.getNetAddress().getRpcPort());
    }
    return 0;
  }

  @Override
  public String getCommandName() {
    return "decommissionWorker";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(HOST_OPTION);
  }

  @Override
  public String getUsage() {
    return "decommissionWorker --h <worker host>";
  }

  @Override
  public String getDescription() {
    return "Decommission a specific worker in the Alluxio cluster. The decommissioned"
        + "worker is not shut down but will not accept new read/write operations. The ongoing "
        + "operations will proceed until completion.";
  }
}
