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
import alluxio.grpc.JournalDomain;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Arrays;

/**
 * Command for removing a server from journal quorum.
 */
public class QuorumRemoveCommand extends AbstractFsAdminCommand {

  public static final String ADDRESS_OPTION_NAME = "address";
  public static final String DOMAIN_OPTION_NAME = "domain";

  public static final String OUTPUT_RESULT = "Removed server at: %s from quorum: %s";

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public QuorumRemoveCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  /**
   * @return command's description
   */
  @VisibleForTesting
  public static String description() {
    return "Removes a server from embedded journal quorum.";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    JournalMasterClient jmClient = mMasterJournalMasterClient;
    String domainVal = cl.getOptionValue(DOMAIN_OPTION_NAME);
    try {
      JournalDomain domain = JournalDomain.valueOf(domainVal);
      if (domain == JournalDomain.JOB_MASTER) {
        jmClient = mJobMasterJournalMasterClient;
      }
    } catch (IllegalArgumentException e) {
      throw new InvalidArgumentException(ExceptionMessage.INVALID_OPTION_VALUE
              .getMessage(DOMAIN_OPTION_NAME, Arrays.toString(JournalDomain.values())));
    }

    String serverAddress = cl.getOptionValue(ADDRESS_OPTION_NAME);
    jmClient.removeQuorumServer(QuorumCommand.stringToAddress(serverAddress));
    mPrintStream.println(String.format(OUTPUT_RESULT, serverAddress, domainVal));

    return 0;
  }

  @Override
  public String getCommandName() {
    return "remove";
  }

  @Override
  public String getUsage() {
    return String.format("%s -%s <MASTER|JOB_MASTER>%n -%s <HostName:Port>", getCommandName(),
        DOMAIN_OPTION_NAME, ADDRESS_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    if (cl.getOptions().length != 2) {
      throw new InvalidArgumentException(
          ExceptionMessage.INVALID_OPTION_COUNT.getMessage(2, cl.getOptions().length));
    }
    // Validate passed options are correct.
    if (!cl.hasOption(DOMAIN_OPTION_NAME) || !cl.hasOption(ADDRESS_OPTION_NAME)) {
      throw new InvalidArgumentException(ExceptionMessage.INVALID_OPTION
          .getMessage(String.format("[%s, %s]", DOMAIN_OPTION_NAME, ADDRESS_OPTION_NAME)));
    }
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(DOMAIN_OPTION_NAME, true, "Journal domain")
        .addOption(ADDRESS_OPTION_NAME, true, "Server address to remove");
  }
}
