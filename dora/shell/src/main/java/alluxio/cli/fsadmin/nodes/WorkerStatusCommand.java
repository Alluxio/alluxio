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

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

/**
 * Command to print worker nodes status.
 */
public class WorkerStatusCommand extends AbstractFsAdminCommand {

  private final AlluxioConfiguration mAlluxioConf;

  /**
   * @param context
   * @param alluxioConf
   */
  public WorkerStatusCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mAlluxioConf = alluxioConf;
  }

  @Override
  public String getCommandName() {
    return "status";
  }

  @Override
  public String getUsage() {
    return "status";
  }

  @Override
  public String getDescription() {
    return "Show all registered workers' status";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    MembershipManager membershipManager = MembershipManager.Factory.create(mAlluxioConf);
    mPrintStream.println(membershipManager.showAllMembers());
    return 0;
  }
}
