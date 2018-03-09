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
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Report Alluxio runtime information.
 */
@ThreadSafe
public final class ReportCommand extends AbstractCommand {

  enum Command {
    SUMMARY, // Reports the Alluxio cluster information
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
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();

    if (args.length == 0) {
      SummaryCommand.printSummary();
      return 0;
    }

    Command command;
    try {
      command = Command.valueOf(args[0].toUpperCase());
    } catch (IllegalArgumentException e) {
      System.out.println(getUsage());
      System.out.println(getDescription());
      return 1;
    }

    switch (command) {
      case SUMMARY:
        SummaryCommand.printSummary();
        break;
      default:
        break;
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "report [Category] [Category_Arguments]";
  }

  @Override
  public String getDescription() {
    return "Report Alluxio running cluster information.\n"
        + "Where Category is an optional argument, if no arguments passed in, "
        + "summary information will be printed out."
        + "Category can be one of the following:\n"
        + "    summary          Alluxio cluster summarized information\n";
  }

  @Override
  public void validateArgs(String... args) throws InvalidArgumentException {
    if (args.length > 1) {
      throw new InvalidArgumentException(
          ExceptionMessage.INVALID_ARGS_GENERIC.getMessage(getCommandName()));
    }
  }
}
