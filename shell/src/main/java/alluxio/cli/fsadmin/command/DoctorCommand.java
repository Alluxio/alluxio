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

import alluxio.cli.Command;
import alluxio.cli.CommandUtils;

import alluxio.cli.fsadmin.FileSystemAdminShellUtils;
import alluxio.cli.fsadmin.command.doctor.ConfigurationCommand;
import alluxio.client.MetaMasterClient;
import alluxio.client.RetryHandlingMetaMasterClient;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.master.MasterClientConfig;

import com.google.common.io.Closer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Shows errors or warnings that users should pay attention to.
 */
public final class DoctorCommand implements Command {
  public static final String HELP_OPTION_NAME = "h";
  private final Closer mCloser;
  private final MetaMasterClient mMetaMasterClient;
  private final PrintStream mPrintStream;

  private static final Option HELP_OPTION =
      Option.builder(HELP_OPTION_NAME)
          .required(false)
          .hasArg(false)
          .desc("print help information.")
          .build();

  enum Command {
    ALL, // Show all errors/warnings
    CONFIGURATION, // Show server-side configuration errors/warnings
  }

  /**
   * Creates a new instance of {@link DoctorCommand}.
   */
  public DoctorCommand() {
    MasterClientConfig config = MasterClientConfig.defaults();
    mCloser = Closer.create();
    mMetaMasterClient = mCloser.register(new RetryHandlingMetaMasterClient(config));
    mPrintStream = System.out;
  }

  @Override
  public String getCommandName() {
    return "doctor";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    try {
      String[] args = cl.getArgs();

      if (cl.hasOption(HELP_OPTION_NAME)) {
        System.out.println(getUsage());
        System.out.println(getDescription());
        return 0;
      }

      if (!FileSystemAdminShellUtils.masterClientServiceIsRunning()) {
        return 1;
      }

      // Get the report category
      Command command = Command.ALL;
      if (args.length == 1) {
        switch (args[0]) {
          case "configuration":
            command = Command.CONFIGURATION;
            break;
          default:
            System.out.println(getUsage());
            System.out.println(getDescription());
            throw new InvalidArgumentException("doctor category is invalid.");
        }
      }

      switch (command) {
        case ALL:
          break;
        case CONFIGURATION:
          ConfigurationCommand configurationCommand = new ConfigurationCommand(
              mMetaMasterClient, mPrintStream);
          configurationCommand.run();
          break;
        default:
          break;
      }
    } finally {
      mCloser.close();
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "doctor [category]";
  }

  @Override
  public String getDescription() {
    return "Show Alluxio errors and warnings.\n"
        + "Where [category] is an optional argument. If no arguments are passed in, "
        + "all the errors/warnings will be printed out.\n"
        + "[category] can be one of the following:\n"
        + "    configuration    server-side configuration errors/warnings\n";
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(HELP_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoMoreThan(this, cl, 1);
  }
}
