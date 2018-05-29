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

import alluxio.cli.CommandUtils;
import alluxio.cli.fsadmin.report.CapacityCommand;
import alluxio.cli.fsadmin.report.ConfigurationCommand;
import alluxio.cli.fsadmin.report.MetricsCommand;
import alluxio.cli.fsadmin.report.SummaryCommand;
import alluxio.cli.fsadmin.report.UfsCommand;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.MasterInquireClient;
import alluxio.master.PollingMasterInquireClient;
import alluxio.resource.CloseableResource;
import alluxio.retry.ExponentialBackoffRetry;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

/**
 * Reports Alluxio running cluster information.
 */
public final class ReportCommand extends AbstractFsAdminCommand {
  public static final String HELP_OPTION_NAME = "h";
  public static final String LIVE_OPTION_NAME = "live";
  public static final String LOST_OPTION_NAME = "lost";
  public static final String SPECIFIED_OPTION_NAME = "workers";

  private static final Option HELP_OPTION =
      Option.builder(HELP_OPTION_NAME)
          .required(false)
          .hasArg(false)
          .desc("print help information.")
          .build();

  private static final Option LIVE_OPTION =
      Option.builder(LIVE_OPTION_NAME)
          .required(false)
          .hasArg(false)
          .desc("show capacity information of live workers.")
          .build();

  private static final Option LOST_OPTION =
      Option.builder(LOST_OPTION_NAME)
          .required(false)
          .hasArg(false)
          .desc("show capacity information of lost workers.")
          .build();

  private static final Option SPECIFIED_OPTION =
      Option.builder(SPECIFIED_OPTION_NAME)
          .required(false)
          .hasArg(true)
          .desc("show capacity information of specified workers.")
          .build();

  enum Command {
    CAPACITY, // Report worker capacity information
    CONFIGURATION, // Report runtime configuration
    METRICS, // Report metrics information
    SUMMARY, // Report cluster summary
    UFS // Report under filesystem information
  }

  /**
   * @param context fsadmin command context
   */
  public ReportCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "report";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();

    if (cl.hasOption(HELP_OPTION_NAME)
        && !(args.length > 0 && args[0].equals("capacity"))) {
      // if category is capacity, we print report capacity usage inside CapacityCommand.
      System.out.println(getUsage());
      System.out.println(getDescription());
      return 0;
    }

    // Get the report category
    Command command = Command.SUMMARY;
    if (args.length == 1) {
      switch (args[0]) {
        case "capacity":
          command = Command.CAPACITY;
          break;
        case "configuration":
          command = Command.CONFIGURATION;
          break;
        case "metrics":
          command = Command.METRICS;
          break;
        case "summary":
          command = Command.SUMMARY;
          break;
        case "ufs":
          command = Command.UFS;
          break;
        default:
          System.out.println(getUsage());
          System.out.println(getDescription());
          throw new InvalidArgumentException("report category is invalid.");
      }
    }

    // Only capacity category has [category args]
    if (!command.equals(Command.CAPACITY)) {
      if (cl.getOptions().length > 0) {
        throw new InvalidArgumentException(
            String.format("report %s does not support arguments: %s",
                command.toString().toLowerCase(), cl.getOptions()[0].getOpt()));
      }
    }

    // Check if Alluxio master and client services are running
    try (CloseableResource<FileSystemMasterClient> client =
             FileSystemContext.get().acquireMasterClientResource()) {
      MasterInquireClient inquireClient = null;
      try {
        InetSocketAddress address = client.get().getAddress();
        List<InetSocketAddress> addresses = Arrays.asList(address);
        inquireClient = new PollingMasterInquireClient(addresses, () ->
            new ExponentialBackoffRetry(50, 100, 2));
      } catch (UnavailableException e) {
        System.err.println("Failed to get the leader master.");
        System.err.println("Please check your Alluxio master status");
        return 1;
      }
      try {
        inquireClient.getPrimaryRpcAddress();
      } catch (UnavailableException e) {
        System.err.println("The Alluxio leader master is not currently serving requests.");
        System.err.println("Please check your Alluxio master status");
        return 1;
      }
    }

    switch (command) {
      case CAPACITY:
        CapacityCommand capacityCommand = new CapacityCommand(
            mBlockClient, mPrintStream);
        capacityCommand.run(cl);
        break;
      case CONFIGURATION:
        ConfigurationCommand configurationCommand = new ConfigurationCommand(
            mMetaClient, mPrintStream);
        configurationCommand.run();
        break;
      case METRICS:
        MetricsCommand metricsCommand = new MetricsCommand(
            mMetaClient, mPrintStream);
        metricsCommand.run();
        break;
      case SUMMARY:
        SummaryCommand summaryCommand = new SummaryCommand(
            mMetaClient, mBlockClient, mPrintStream);
        summaryCommand.run();
        break;
      case UFS:
        UfsCommand ufsCommand = new UfsCommand(mFsClient);
        ufsCommand.run();
        break;
      default:
        break;
    }
    return 0;
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(HELP_OPTION)
        .addOption(LIVE_OPTION)
        .addOption(LOST_OPTION)
        .addOption(SPECIFIED_OPTION);
  }

  @Override
  public String getUsage() {
    return usage();
  }

  /**
   * @return the usage for the report command
   */
  @VisibleForTesting
  public static String usage() {
    return "report [category] [category args]";
  }

  @Override
  public String getDescription() {
    return description();
  }

  /**
   * @return the description for the report command
   */
  @VisibleForTesting
  public static String description() {
    return "Report Alluxio running cluster information.\n"
        + "Where [category] is an optional argument. If no arguments are passed in, "
        + "summary information will be printed out.\n"
        + "[category] can be one of the following:\n"
        + "    capacity         worker capacity information\n"
        + "    configuration    runtime configuration\n"
        + "    metrics          metrics information\n"
        + "    summary          cluster summary\n"
        + "    ufs              under filesystem information\n";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoMoreThan(this, cl, 1);
  }
}
