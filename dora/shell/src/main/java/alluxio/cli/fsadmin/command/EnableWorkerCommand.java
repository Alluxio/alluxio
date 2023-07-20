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

package alluxio.cli.fsadmin.command;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.RemoveDisabledWorkerPOptions;
import alluxio.wire.WorkerNetAddress;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The DecommissionWorkerCommand can specify to disable certain workers in the cluster, so
 * they are not allowed to register again. This command is the reverse operation of that, which
 * enables those workers to register to the cluster.
 * See the help message for more details.
 */
public class EnableWorkerCommand extends AbstractFsAdminCommand {
  private static final Option ADDRESSES_OPTION =
      Option.builder("a")
          .longOpt("addresses")
          .required(true)  // Host option is mandatory.
          .hasArg(true)
          .numberOfArgs(1)
          .argName("workerHosts")
          .desc("One or more worker addresses separated by comma. If port is not specified, "
              + PropertyKey.WORKER_WEB_PORT.getName() + " will be used. "
              + "Note the addresses specify the WEB port instead of RPC port!")
          .build();

  private final AlluxioConfiguration mConf;

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public EnableWorkerCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mConf = alluxioConf;
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String workerAddressesStr = cl.getOptionValue(ADDRESSES_OPTION.getLongOpt());
    if (workerAddressesStr.isEmpty()) {
      throw new IllegalArgumentException("Worker addresses must be specified");
    }
    List<WorkerNetAddress> addresses = WorkerAddressUtils.parseWorkerAddresses(
        workerAddressesStr, mConf);

    Set<WorkerNetAddress> failedWorkers = new HashSet<>();
    for (WorkerNetAddress workerAddress : addresses) {
      System.out.format("Re-enabling worker %s%n",
          WorkerAddressUtils.convertAddressToStringWebPort(workerAddress));
      try {
        RemoveDisabledWorkerPOptions options =
                RemoveDisabledWorkerPOptions.newBuilder()
                .setWorkerHostname(workerAddress.getHost())
                .setWorkerWebPort(workerAddress.getWebPort()).build();
        mBlockClient.removeDisabledWorker(options);
        System.out.format("Re-enabled worker %s on master%n",
            WorkerAddressUtils.convertAddressToStringWebPort(workerAddress));
      } catch (IOException ie) {
        System.err.format("Failed to re-enable worker %s%n",
            WorkerAddressUtils.convertAddressToStringWebPort(workerAddress));
        ie.printStackTrace();
        failedWorkers.add(workerAddress);
      }
    }

    if (failedWorkers.size() == 0) {
      System.out.println("Successfully re-enabled all workers on the master. "
          + "The workers should be able to register to the master and then serve normally."
          + "Note there is a short gap defined by "
          + PropertyKey.USER_WORKER_LIST_REFRESH_INTERVAL.getName()
          + " before the clients become aware of this worker and start to use it.");
      return 0;
    } else {
      System.out.format("%s failed to be re-enabled on the master: %s%n", failedWorkers.size(),
          failedWorkers.stream().map(WorkerAddressUtils::convertAddressToStringWebPort)
              .collect(Collectors.toList()));
      System.out.println("The admin needs to manually check and fix the problem. "
          + "Those workers are not able to register to the master and serve requests.");
      return 1;
    }
  }

  @Override
  public String getCommandName() {
    return "enableWorker";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(ADDRESSES_OPTION);
  }

  @Override
  public String getUsage() {
    return "enableWorker --addresses <workerHosts>";
  }

  @Override
  public String getDescription() {
    return "Re-enables workers to register and join the cluster. This is used in pair with the "
        + "decommissionWorker command. For example:\n\n"
        + "# -d specifies the worker should be rejected from registering\n"
        + "$bin/alluxio fsadmin decommissionWorker --addresses worker1 -d\n"
        + "# This can be reversed by enableWorker command\n"
        + "$bin/alluxio fsadmin enableWorker --addresses worker1\n"
        + "# worker1 should now be able to register";
  }
}
