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
import alluxio.cli.fsadmin.command.report.SummaryCommand;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.MasterInquireClient;
import alluxio.master.PollingMasterInquireClient;
import alluxio.resource.CloseableResource;
import alluxio.retry.ExponentialBackoffRetry;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Report Alluxio runtime information.
 */
@ThreadSafe
public final class ReportCommand extends AbstractCommand {

  enum Command {
    SUMMARY // Reports the Alluxio cluster information
  }

  /**
   * Creates a new instance of {@link ReportCommand}.
   */
  public ReportCommand() {}

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
    // Checks if Alluxio master and client services are running
    try (CloseableResource<FileSystemMasterClient> client =
             FileSystemContext.INSTANCE.acquireMasterClientResource()) {
      InetSocketAddress address = client.get().getAddress();
      List<InetSocketAddress> addresses = Arrays.asList(address);
      MasterInquireClient inquireClient = new PollingMasterInquireClient(addresses, () ->
          new ExponentialBackoffRetry(50, 100, 2)
      );
      try {
        inquireClient.getPrimaryRpcAddress();
      } catch (UnavailableException e) {
        System.err.println("The Alluxio leader master is not currently serving requests.");
        System.err.println("Please check your Alluxio master status");
        return 1;
      }
    } catch (UnavailableException e) {
      System.err.println("Failed to get the leader master.");
      System.err.println("Please check your Alluxio master status");
      return 1;
    }

    String[] args = cl.getArgs();

    if (args.length == 0) {
      SummaryCommand summaryCommand = new SummaryCommand();
      summaryCommand.run(cl);
      return 0;
    }

    // Gets the report category
    Command command;
    try {
      String commandName = args[0].toUpperCase();
      command = Command.valueOf(commandName);
    } catch (IllegalArgumentException e) {
      System.out.println(getUsage());
      System.out.println(getDescription());
      return -1;
    }

    switch (command) {
      case SUMMARY:
        SummaryCommand summaryCommand = new SummaryCommand();
        summaryCommand.run(cl);
        break;
      // CAPACITY, CONFIGURATION, RPC, OPERATION, and UFS commands will be supported in the future
      default:
        break;
    }
    return 0;
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
        + "    summary          Summarized Alluxio cluster information";
  }

  @Override
  public void validateArgs(String... args) throws InvalidArgumentException {
    if (args.length > 1) {
      throw new InvalidArgumentException(
          ExceptionMessage.INVALID_ARGS_GENERIC.getMessage(getCommandName()));
    }
  }
}
