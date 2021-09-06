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
import alluxio.client.file.FileSystemContext;
import alluxio.client.journal.JournalMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.NetAddress;
import alluxio.master.MasterInquireClient;
import alluxio.util.CommonUtils;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Command for transferring the leadership to another master within a quorum.
 */
public class QuorumElectCommand extends AbstractFsAdminCommand {

  public static final String ADDRESS_OPTION_NAME = "address";

  public static final String TRANSFER_SUCCESS = "Transferred leadership to server: %s";
  public static final String TRANSFER_FAILED = "Leadership was not transferred to %s: %s";
  public static final String RESET_SUCCESS = "Quorum priorities were reset to 1";
  public static final String RESET_FAILED = "Quorum priorities failed to be reset: %s";

  private final AlluxioConfiguration mConf;

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public QuorumElectCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mConf = alluxioConf;
  }

  /**
   * @return command's description
   */
  @VisibleForTesting
  public static String description() {
    return "Transfers leadership of the quorum to the master located at <hostname>:<port>";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    JournalMasterClient jmClient = mMasterJournalMasterClient;
    String serverAddress = cl.getOptionValue(ADDRESS_OPTION_NAME);
    NetAddress address = QuorumCommand.stringToAddress(serverAddress);

    jmClient.transferLeadership(address);

    MasterInquireClient inquireClient = MasterInquireClient.Factory
            .create(mConf, FileSystemContext.create(mConf).getClientContext().getUserState());
    boolean success = true;
    // wait for confirmation of leadership transfer
    try {
      CommonUtils.waitFor("Waiting for leadership transfer to finalize", () -> {
        InetSocketAddress leaderAddress;
        try {
          leaderAddress = inquireClient.getPrimaryRpcAddress();
        } catch (UnavailableException e) {
          return false;
        }
        return leaderAddress.getHostName().equals(address.getHost())
                && leaderAddress.getPort() == address.getRpcPort();
      });
      mPrintStream.println(String.format(TRANSFER_SUCCESS, serverAddress));
    } catch (Exception e) {
      success = false;
      mPrintStream.println(String.format(TRANSFER_FAILED, serverAddress, e));
    }
    // Resetting RaftPeer priorities using a separate RPC because the old leader has shut down
    // its RPC server. We want to reset them regardless of transfer success because the original
    // setting of priorities may have succeeded while the transfer might not have.
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
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    if (!cl.hasOption(ADDRESS_OPTION_NAME)) {
      throw new InvalidArgumentException(ExceptionMessage.INVALID_OPTION
              .getMessage(String.format("[%s]", ADDRESS_OPTION_NAME)));
    }
  }

  @Override
  public String getCommandName() {
    return "elect";
  }

  @Override
  public String getUsage() {
    return String.format("%s -%s <HOSTNAME:PORT>", getCommandName(), ADDRESS_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(ADDRESS_OPTION_NAME, true,
            "Server address that will take over as leader");
  }
}
