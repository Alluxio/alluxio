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

import alluxio.ClientContext;
import alluxio.cli.CommandUtils;
import alluxio.cli.fsadmin.command.AbstractFsAdminCommand;
import alluxio.cli.fsadmin.command.Context;
import alluxio.client.meta.MetaMasterClient;
import alluxio.client.meta.RetryHandlingMetaMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.master.MasterClientContext;
import alluxio.master.selectionpolicy.MasterSelectionPolicy;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Command for triggering a checkpoint in the primary master journal system.
 */
public class CheckpointCommand extends AbstractFsAdminCommand {
  public static final String ADDRESS_OPTION_NAME = "address";

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public CheckpointCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "checkpoint";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    Thread thread = CommonUtils.createProgressThread(System.out);
    thread.start();
    try {
      if (cl.hasOption(ADDRESS_OPTION_NAME)) {
        String strAddr = cl.getOptionValue(ADDRESS_OPTION_NAME);
        InetSocketAddress address = NetworkAddressUtils.parseInetSocketAddress(strAddr);
        MasterSelectionPolicy policy = MasterSelectionPolicy.Factory.specifiedMaster(address);
        MasterClientContext context = MasterClientContext
            .newBuilder(ClientContext.create(Configuration.global())).build();
        try (MetaMasterClient client = new RetryHandlingMetaMasterClient(context, policy)) {
          client.connect();
          client.checkpoint();
        }
        mPrintStream.printf("Successfully took a checkpoint on master %s%n", strAddr);
      } else {
        String masterHostname = mMetaClient.checkpoint();
        mPrintStream.printf("Successfully took a checkpoint on master %s%n", masterHostname);
      }
    } finally {
      thread.interrupt();
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s [-%s <HOSTNAME:PORT>]", getCommandName(), ADDRESS_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return "creates a checkpoint in the primary master journal system. This command "
        + "is mainly used for debugging and to avoid master journal logs "
        + "from growing unbounded. Checkpointing requires a pause in master metadata changes, "
        + "so use this command sparingly to avoid interfering with other users of the system. "
        + "The '-address' option can direct the checkpoint taking to a specific master.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoMoreThan(this, cl, 1);
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(ADDRESS_OPTION_NAME, true,
        "Server address of the master taking the checkpoint");
  }
}
