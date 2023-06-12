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
import alluxio.grpc.GetQuorumInfoPResponse;
import alluxio.grpc.NetAddress;
import alluxio.grpc.QuorumServerInfo;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Command for transferring the leadership to another master within a quorum.
 */
public class QuorumElectCommand extends AbstractFsAdminCommand {

  public static final String ADDRESS_OPTION_NAME = "address";

  public static final String TRANSFER_INIT = "Initiating transfer of leadership to %s";
  public static final String TRANSFER_SUCCESS = "Successfully elected %s as the new leader";
  public static final String TRANSFER_FAILED = "Failed to elect %s as the new leader: %s";

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public QuorumElectCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
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
    try {
      mPrintStream.println(String.format(TRANSFER_INIT, serverAddress));
      String transferId = jmClient.transferLeadership(address);
      AtomicReference<String> errorMessage = new AtomicReference<>("");
      // wait for confirmation of leadership transfer
      final int TIMEOUT_3MIN = 3 * 60 * 1000; // in milliseconds
      CommonUtils.waitFor("election to finalize.", () -> {
        try {
          errorMessage.set(jmClient.getTransferLeaderMessage(transferId).getTransMsg().getMsg());
          if (!errorMessage.get().isEmpty()) {
            // if an error is reported, end the retry immediately
            return true;
          }
          GetQuorumInfoPResponse quorumInfo = jmClient.getQuorumInfo();
          Optional<QuorumServerInfo> leadingMasterInfoOpt = quorumInfo.getServerInfoList().stream()
              .filter(QuorumServerInfo::getIsLeader).findFirst();
          return leadingMasterInfoOpt.isPresent()
              && address.equals(leadingMasterInfoOpt.get().getServerAddress());
        } catch (IOException e) {
          return false;
        }
      }, WaitForOptions.defaults().setTimeoutMs(TIMEOUT_3MIN));

      if (!errorMessage.get().isEmpty()) {
        throw new Exception(errorMessage.get());
      }
      mPrintStream.println(String.format(TRANSFER_SUCCESS, serverAddress));
    } catch (Exception e) {
      mPrintStream.println(String.format(TRANSFER_FAILED, serverAddress, e.getMessage()));
      return -1;
    }
    return 0;
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
