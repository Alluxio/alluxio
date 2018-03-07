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

package alluxio.cli;

import alluxio.cli.report.SummaryCommand;
import alluxio.util.ConfigurationUtils;

/**
 * Shell to report the Alluxio cluster information.
 */
public final class Report {

  private static final String USAGE = "USAGE:alluxio report \n\n"
      + "report Alluxio running cluster summarization"
      + "-h  display this help.";

  enum Command {
    SUMMARY, // Reports the Alluxio cluster information
  }

  /**
   * Implements report command.
   *
   * @param args the report command and command arguments
   * @return 0 on success, 1 on failures
   */
  private int run(String[] args) {
    if (args.length == 0) {
      SummaryCommand.printSummary();
      return 0;
    }

    for (String arg : args) {
      if (arg.equals("-h")) {
        System.out.println(USAGE);
        return 0;
      }
    }

    Command command;
    try {
      command = Command.valueOf(args[0].toUpperCase());
    } catch (IllegalArgumentException e) {
      System.out.println(USAGE);
      System.out.println(e);
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

  /**
   * Report the information of Alluxio running cluster.
   *
   * @param args the report command and command arguments
   */
  public static void main(String[] args) {
    if (!ConfigurationUtils.masterHostConfigured()) {
      System.out.println("Cannot run report shell as master hostname is not configured.");
      System.exit(1);
    }

    Report report = new Report();
    System.exit(report.run(args));
  }
}
