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
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.NetAddress;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

/**
 * Command for transferring the leadership to another master within a quorum.
 */
public class QuorumTransferLeaderCommand extends AbstractFsAdminCommand {
  public static final String OUTPUT_RESULT = "Transferred leadership to server: %s";

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public QuorumTransferLeaderCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    JournalMasterClient jmClient = mMasterJournalMasterClient;
    String serverAddress = cl.getArgList().get(0);
    NetAddress address = QuorumCommand.stingToAddress(serverAddress);

    jmClient.transferLeadership(address);
    mPrintStream.println(String.format(OUTPUT_RESULT, serverAddress));
    return 0;
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    if (cl.getArgList().size() != 1) {
      throw new InvalidArgumentException(
              ExceptionMessage.INVALID_ARGS_NUM.getMessage(1, cl.getArgs().length)
      );
    }
  }

  @Override
  public String getCommandName() {
    return "transferLeader";
  }

  @Override
  public String getUsage() {
    return String.format("%s <HostName:Port>", getCommandName());
  }

  @Override
  public String getDescription() {
    return "Transfers leadership of the quorum to the master located at <hostname>:<port>";
  }
}
