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

package alluxio.cli.fsadmin.metric;

import alluxio.cli.CommandUtils;
import alluxio.cli.fsadmin.command.AbstractFsAdminCommand;
import alluxio.cli.fsadmin.command.Context;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.ClearMetricsRequest;
import alluxio.wire.WorkerNetAddress;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Clear the leading master (and workers) metrics.
 */
public final class ClearCommand extends AbstractFsAdminCommand {
  public static final String ALL_OPTION_NAME = "all";

  private static final Option ALL_OPTION =
      Option.builder()
          .longOpt(ALL_OPTION_NAME)
          .required(false)
          .hasArg(false)
          .desc("Clear the metrics of leading master and active workers")
          .build();

  private final AlluxioConfiguration mAlluxioConf;

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public ClearCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mAlluxioConf = alluxioConf;
  }

  @Override
  public String getCommandName() {
    return "clear";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(ALL_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoMoreThan(this, cl, 1);
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    System.out.print(String.format("Clearing metrics of Alluxio leading master ..."));
    mMetricsClient.clearMetrics();
    System.out.println("CLEARED！");
    if (cl.hasOption(ALL_OPTION_NAME)) {
      try (FileSystemContext context = FileSystemContext.create(mAlluxioConf)) {
        AlluxioBlockStore store = AlluxioBlockStore.create(FileSystemContext.create(mAlluxioConf));
        List<WorkerNetAddress> addressList = store.getEligibleWorkers().stream()
            .map(BlockWorkerInfo::getNetAddress).collect(Collectors.toList());
        for (WorkerNetAddress worker : addressList) {
          System.out.printf("Clearing metrics of worker %s ...", worker.getHost());
          BlockWorkerClient blockWorkerClient = context.acquireBlockWorkerClient(worker);
          try {
            blockWorkerClient.clearMetrics(ClearMetricsRequest.newBuilder().build());
          } finally {
            context.releaseBlockWorkerClient(worker, blockWorkerClient);
          }
          System.out.println("CLEARED！");
        }
      }
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s [--%s]%n"
            + "\t--%s: %s",
        getCommandName(), ALL_OPTION_NAME,
        ALL_OPTION_NAME, ALL_OPTION.getDescription());
  }

  /**
   * @return command's description
   */
  @VisibleForTesting
  public static String description() {
    return "Clear the metrics of the leading master or the whole cluster. "
        + "This command is useful when getting metrics information in short-term testing."
        + "This command should be used sparingly as it may "
        + "affect the current metrics recording and cause some metrics bias. ";
  }

  @Override
  public String getDescription() {
    return description();
  }
}
