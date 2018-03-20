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

import alluxio.cli.AbstractCommand;
import alluxio.cli.fsadmin.report.CapacityCommand;
import alluxio.cli.fsadmin.report.SummaryCommand;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.RetryHandlingBlockMasterClient;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.MetaMasterClient;
import alluxio.client.RetryHandlingMetaMasterClient;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.MasterClientConfig;
import alluxio.master.MasterInquireClient;
import alluxio.master.PollingMasterInquireClient;
import alluxio.resource.CloseableResource;
import alluxio.retry.ExponentialBackoffRetry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

/**
 * Reports Alluxio running cluster information.
 */
public final class ReportCommand extends AbstractCommand {
  private MetaMasterClient mMetaMasterClient;
  private BlockMasterClient mBlockMasterClient;
  private PrintStream mPrintStream;

  private static final Option LIVE_OPTION =
      Option.builder("live")
          .required(false)
          .hasArg(false)
          .desc("show capacity information of live workers.")
          .build();

  private static final Option LOST_OPTION =
      Option.builder("lost")
          .required(false)
          .hasArg(false)
          .desc("show capacity information of lost workers.")
          .build();

  private static final Option SPECIFIED_OPTION =
      Option.builder("worker")
          .required(false)
          .hasArg(true)
          .desc("show capacity information of specified workers.")
          .build();

  private static final Option HELP_OPTION =
      Option.builder("h")
          .required(false)
          .hasArg(false)
          .desc("print help information.")
          .build();

  enum Command {
    SUMMARY, // Report the Alluxio cluster information
    CAPACITY // Report the capacity information
  }

  /**
   * Creates a new instance of {@link ReportCommand}.
   */
  public ReportCommand() {
    mMetaMasterClient = new RetryHandlingMetaMasterClient(MasterClientConfig.defaults());
    mBlockMasterClient = new RetryHandlingBlockMasterClient(MasterClientConfig.defaults());
    mPrintStream = System.out;
  }

  @Override
  public String getCommandName() {
    return "report";
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    if (cl.hasOption("h")
        && !(args.length > 0 && args[0].equalsIgnoreCase("CAPACITY"))) {
      System.out.println(getUsage());
      System.out.println(getDescription());
      return 0;
    }

    try {
      // Check if Alluxio master and client services are running
      try (CloseableResource<FileSystemMasterClient> client =
               FileSystemContext.INSTANCE.acquireMasterClientResource()) {
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

      // Print the summarized information in default situation
      if (args.length == 0) {
        SummaryCommand summaryCommand = new SummaryCommand(mMetaMasterClient,
            mBlockMasterClient, mPrintStream);
        summaryCommand.run();
        return 0;
      }

      // Get the report category
      Command command;
      try {
        String commandName = args[0].toUpperCase();
        command = Command.valueOf(commandName);
      } catch (IllegalArgumentException e) {
        System.out.println(getUsage());
        System.out.println(getDescription());
        return 1;
      }

      switch (command) {
        case SUMMARY:
          SummaryCommand summaryCommand = new SummaryCommand(mMetaMasterClient,
              mBlockMasterClient, mPrintStream);
          summaryCommand.run();
          break;
        case CAPACITY:
          CapacityCommand capacityCommand = new CapacityCommand(
              mBlockMasterClient, mPrintStream);
          capacityCommand.run(cl);
          break;
        default:
          break;
      }
    } finally {
      mMetaMasterClient.close();
      mBlockMasterClient.close();
    }
    return 0;
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(LIVE_OPTION)
        .addOption(LOST_OPTION)
        .addOption(SPECIFIED_OPTION)
        .addOption(HELP_OPTION);
  }

  @Override
  public String getUsage() {
    return "report [category] [category args]";
  }

  @Override
  public String getDescription() {
    return "Report Alluxio running cluster information.\n"
        + "Where category is an optional argument, if no arguments are passed in, "
        + "summary information will be printed out."
        + "category can be one of the following:\n"
        + "    summary          Summarized Alluxio cluster information\n"
        + "    capacity         Capacity usage information";
  }

  @Override
  public void validateArgs(String... args) throws InvalidArgumentException {
    if (args.length > 1) {
      throw new InvalidArgumentException(
          ExceptionMessage.INVALID_ARGS_GENERIC.getMessage(getCommandName()));
    }
  }
}
