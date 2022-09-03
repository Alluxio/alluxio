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
import alluxio.client.block.options.GetWorkerReportOptions.WorkerInfoField;
import alluxio.conf.AlluxioConfiguration;
import alluxio.wire.WorkerInfo;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Delete a worker from the cluster.
 */
public class DeleteWorkerCommand extends AbstractFsAdminCommand {
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
          .desc("force a worker to be deleted even if the worker is not a lost worker.")
          .build();

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(HELP_OPTION)
        .addOption(FORCE_FLAG);
  }

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public DeleteWorkerCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "deleteWorker";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    if (cl.hasOption(HELP_OPTION.getLongOpt())) {
      System.out.println(getUsage());
    }

    String[] args = cl.getArgs();
    if (args.length != 1) {
      System.out.println(getUsage());
      return 0;
    }
    boolean force = cl.hasOption(FORCE_FLAG.getLongOpt());
    String address = args[0];
    GetWorkerReportOptions options =
        getReportOptions(new HashSet<>(Arrays.asList(address.split(","))));
    List<WorkerInfo> workerInfoList = null;
    try {
      workerInfoList = mBlockClient.getWorkerReport(options);
      if (workerInfoList.size() == 0) {
        System.out.println(address + " is not a valid worker name");
      }
      WorkerInfo workerInfo = workerInfoList.get(0);
      if (force || workerInfo.getState().equals(LOST_WORKER_STATE)) {
        mBlockClient.deleteWorker(workerInfo.getId());
        System.out.println(workerInfo.getAddress().getHost() + "has been deleted");
      } else {
        System.out.println(workerInfo.getAddress().getHost()
            + " is not a LOST Worker, Worker status: " + workerInfo.getState());
      }
    } catch (IOException e) {
      System.out.println("getWorker " + address + "failed");
      return -1;
    }
    return 0;
  }

  private GetWorkerReportOptions getReportOptions(Set<String> addresses) throws IOException {
    GetWorkerReportOptions workerOptions = GetWorkerReportOptions.defaults();

    Set<WorkerInfoField> fieldRange = new HashSet<>();
    fieldRange.add(WorkerInfoField.ADDRESS);
    fieldRange.add(WorkerInfoField.ID);
    fieldRange.add(WorkerInfoField.STATE);

    /*
    These two fields are required because in the definition of WorkerInfo's protobuf,
    these two fields are not optional
    */
    fieldRange.add(WorkerInfoField.WORKER_USED_BYTES_ON_TIERS);
    fieldRange.add(WorkerInfoField.WORKER_CAPACITY_BYTES_ON_TIERS);

    workerOptions.setFieldRange(fieldRange);
    workerOptions.setWorkerRange(GetWorkerReportOptions.WorkerRange.SPECIFIED);
    workerOptions.setAddresses(addresses);
    return workerOptions;
  }

  /**
   * @return the description for the command
   */
  @VisibleForTesting
  public static String usage() {
    return "deleteWorker [WorkerAddresses]";
  }

  @Override
  public String getUsage() {
    return usage();
  }

  @Override
  public String getDescription() {
    return "Delete a worker from the cluster. The data in the worker will not be deleted";
  }
}
