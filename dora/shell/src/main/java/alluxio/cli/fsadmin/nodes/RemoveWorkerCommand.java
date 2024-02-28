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

package alluxio.cli.fsadmin.nodes;

import alluxio.cli.fsadmin.command.AbstractFsAdminCommand;
import alluxio.cli.fsadmin.command.Context;
import alluxio.conf.AlluxioConfiguration;
import alluxio.membership.MembershipManager;
import alluxio.wire.WorkerIdentity;
import alluxio.wire.WorkerInfo;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

/**
 * Remove a non-running worker from the membership.
 */
public class RemoveWorkerCommand extends AbstractFsAdminCommand {
  public static final String WORKERNAME_OPTION_NAME = "n";
  public static final String HELP_OPTION_NAME = "h";

  private static final Option WORKERNAME_OPTION =
      Option.builder(WORKERNAME_OPTION_NAME)
          .required(true)
          .hasArg(true)
          .desc("ID of the worker to remove")
          .build();

  private static final Option HELP_OPTION =
      Option.builder(HELP_OPTION_NAME)
          .required(false)
          .hasArg(false)
          .desc("print help information.")
          .build();

  private final AlluxioConfiguration mAlluxioConf;

  /**
   * @param context
   * @param alluxioConf
   */
  public RemoveWorkerCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mAlluxioConf = alluxioConf;
  }

  @Override
  public String getCommandName() {
    return "remove";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(WORKERNAME_OPTION)
        .addOption(HELP_OPTION);
  }

  @Override
  public String getUsage() {
    return getCommandName() + " -n <WorkerId> | -h";
  }

  @Override
  public String getDescription() {
    return "Remove given worker from the cluster, so that clients and "
        + "other workers will not consider the removed worker for services. "
        + "The worker must have been stopped before it can be safely removed "
        + "from the cluster.";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    if (cl.hasOption(HELP_OPTION_NAME)
        || !cl.hasOption(WORKERNAME_OPTION_NAME)) {
      System.out.println(getUsage());
      System.out.println(getDescription());
      return 0;
    }
    MembershipManager membershipManager =
        MembershipManager.Factory.create(mAlluxioConf);
    String workerId = cl.getOptionValue(WORKERNAME_OPTION_NAME).trim();
    membershipManager.decommission(
        new WorkerInfo().setIdentity(WorkerIdentity.fromString(workerId)));
    mPrintStream.println(String.format(
        "Successfully removed worker: %s", workerId));
    return 0;
  }
}
