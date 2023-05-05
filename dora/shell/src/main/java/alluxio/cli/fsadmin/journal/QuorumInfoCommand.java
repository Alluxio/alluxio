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
import alluxio.grpc.JournalDomain;
import alluxio.grpc.NetAddress;
import alluxio.grpc.QuorumServerInfo;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Command for querying journal quorum information.
 */
public class QuorumInfoCommand extends AbstractFsAdminCommand {
  public static final String DOMAIN_OPTION_NAME = "domain";

  public static final String OUTPUT_HEADER_DOMAIN = "Journal domain\t: %s";
  public static final String OUTPUT_HEADER_QUORUM_SIZE = "Quorum size\t: %d";
  public static final String OUTPUT_HEADER_LEADING_MASTER = "Quorum leader\t: %s";
  public static final String OUTPUT_SERVER_INFO = "%-11s | %-8s | %s%n";

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public QuorumInfoCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  /**
   * @return command's description
   */
  @VisibleForTesting
  public static String description() {
    return "Shows quorum information for embedded journal.";
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

    GetQuorumInfoPResponse quorumInfo = jmClient.getQuorumInfo();

    Optional<QuorumServerInfo> leadingMasterInfoOpt = quorumInfo.getServerInfoList().stream()
            .filter(QuorumServerInfo::getIsLeader).findFirst();
    String leadingMasterAddr = leadingMasterInfoOpt.isPresent()
            ? netAddressToString(leadingMasterInfoOpt.get().getServerAddress()) : "UNKNOWN";

    List<String[]> table = quorumInfo.getServerInfoList().stream().map(info -> new String[]{
            info.getServerState().toString(),
            Integer.toString(info.getPriority()),
            netAddressToString(info.getServerAddress()),
    }).collect(Collectors.toList());
    table.add(0, new String[]{"STATE", "PRIORITY", "SERVER ADDRESS"});

    mPrintStream.println(String.format(OUTPUT_HEADER_DOMAIN, quorumInfo.getDomain()));
    mPrintStream
        .println(String.format(OUTPUT_HEADER_QUORUM_SIZE, quorumInfo.getServerInfoList().size()));
    mPrintStream.println(String.format(OUTPUT_HEADER_LEADING_MASTER, leadingMasterAddr));
    mPrintStream.println();
    for (String[] output : table) {
      mPrintStream.printf(OUTPUT_SERVER_INFO, output);
    }
    return 0;
  }

  String netAddressToString(NetAddress address) {
    return String.format("%s:%d", address.getHost(), address.getRpcPort());
  }

  @Override
  public String getCommandName() {
    return "info";
  }

  @Override
  public String getUsage() {
    return String.format("%s -%s <%s>%n", getCommandName(), DOMAIN_OPTION_NAME,
        Arrays.toString(JournalDomain.values()));
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    if (cl.getOptions().length != 1) {
      throw new InvalidArgumentException(
          ExceptionMessage.INVALID_OPTION_COUNT.getMessage(1, cl.getOptions().length));
    }
    // Validate passed options are correct.
    if (!cl.hasOption(DOMAIN_OPTION_NAME)) {
      throw new InvalidArgumentException(
          ExceptionMessage.INVALID_OPTION.getMessage(String.format("[%s]", DOMAIN_OPTION_NAME)));
    }
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(DOMAIN_OPTION_NAME, true, "Journal domain");
  }
}
