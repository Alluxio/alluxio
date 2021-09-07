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

package alluxio.cli.fsadmin.journal;

import alluxio.cli.fsadmin.command.AbstractFsAdminCommand;
import alluxio.cli.fsadmin.command.Context;
import alluxio.client.journal.JournalMasterClient;

import alluxio.conf.AlluxioConfiguration;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;

/**
 * Command for resetting the priority of raft group member.
 */
public class QuorumResetPriorityCommand extends AbstractFsAdminCommand {
  public static final String RESET_SUCCESS = "Quorum priorities were reset to 1";
  public static final String RESET_FAILED = "Quorum priorities failed to be reset: %s";

  /**
   * @param context fsadmin command context
   */
  protected QuorumResetPriorityCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  /**
   * @return command's description
   */
  @VisibleForTesting
  public static String description() {
    return "Resetting the priority of raft group member.";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    JournalMasterClient jmClient = mMasterJournalMasterClient;
    boolean success = true;
    try {
      jmClient.resetPriorities();
      mPrintStream.println(RESET_SUCCESS);
    } catch (Exception e) {
      success = false;
      mPrintStream.println(String.format(RESET_FAILED, e));
    }
    return success ? 0 : -1;
  }

  @Override
  public String getCommandName() {
    return "reset";
  }

  @Override
  public String getUsage() {
    return String.format("%s", getCommandName());
  }

  @Override
  public String getDescription() {
    return description();
  }
}
