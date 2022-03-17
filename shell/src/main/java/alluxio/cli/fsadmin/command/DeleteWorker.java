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

import static alluxio.Constants.LOST_WORKER_STATE;

import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.client.block.options.GetWorkerReportOptions.WorkerInfoField;
import alluxio.wire.WorkerInfo;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * todo.
 */
public class DeleteWorker extends AbstractFsAdminCommand {
  private static final Option HELP_OPTION =
      Option.builder()
          .required(false)
          .hasArg(false)
          .longOpt("help")
          .desc("print help information.")
          .build();
  private static final Option FORCE_FLAG =
      Option.builder()
          .required(false)
          .hasArg(false)
          .longOpt("force")
          .desc("print help information.")
          .build();

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(HELP_OPTION)
        .addOption(FORCE_FLAG);
  }

  /**
   * todo.
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public DeleteWorker(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "deleteWorker";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    if (cl.hasOption(HELP_OPTION.getLongOpt())) {
      System.out.println(getUsage());
    }

    String[] args = cl.getArgs();
    if (args.length != 1) { // todo 1
      System.out.println(getUsage());
      return 0;
    }
    boolean force = cl.hasOption(FORCE_FLAG.getLongOpt());
    String address = args[0];
    GetWorkerReportOptions options = getOptions(new HashSet<>(Arrays.asList(address.split(","))));
    List<WorkerInfo> workerInfoList = mBlockClient.getWorkerReport(options);
    // TODO issues#11172: If the worker is in a container, use the container hostname
    for (WorkerInfo workerInfo: workerInfoList) {
      if (force || workerInfo.getState().equals(LOST_WORKER_STATE)) {
        mBlockClient.deleteWorker(workerInfo.getId());
      } else {
        System.out.println(workerInfo.getAddress().getHost() + " " + workerInfo.getState());
      }
    }

    return 0;
  }

  private GetWorkerReportOptions getOptions(Set<String> addresses) throws IOException {
    GetWorkerReportOptions workerOptions = GetWorkerReportOptions.defaults();

    Set<WorkerInfoField> fieldRange = new HashSet<>();
    fieldRange.add(WorkerInfoField.ADDRESS);
    fieldRange.add(WorkerInfoField.ID);
    fieldRange.add(WorkerInfoField.STATE);
    /*todo comments*/
    fieldRange.add(WorkerInfoField.WORKER_USED_BYTES_ON_TIERS);
    fieldRange.add(WorkerInfoField.WORKER_CAPACITY_BYTES_ON_TIERS);
    workerOptions.setFieldRange(fieldRange);
    workerOptions.setWorkerRange(GetWorkerReportOptions.WorkerRange.SPECIFIED);
    workerOptions.setAddresses(addresses);
    return workerOptions;
  }

  @Override
  public String getUsage() {
    return "deleteWorker [path]";
  }

  @Override
  public String getDescription() {
    return "statelock returns all waiters and holders of the state lock.";
  }
}
