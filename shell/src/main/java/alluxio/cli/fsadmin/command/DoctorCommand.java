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
import alluxio.cli.fsadmin.FileSystemAdminShellUtils;
import alluxio.cli.fsadmin.doctor.ConfigurationCommand;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

/**
 * Shows errors or warnings that users should pay attention to.
 */
public final class DoctorCommand extends AbstractFsAdminCommand {
  public static final String HELP_OPTION_NAME = "h";

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
   * @param context fsadmin command context
   */
  public DoctorCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "doctor";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();

    if (cl.hasOption(HELP_OPTION_NAME)) {
      System.out.println(getUsage());
      System.out.println(getDescription());
      return 0;
    }

    FileSystemAdminShellUtils.checkMasterClientService();

    // Get the doctor category
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
      case ALL:// intended to fall through
        // TODO(lu) add other Alluxio errors and warnings and separate from CONFIGURATION
      case CONFIGURATION:
        ConfigurationCommand configurationCommand =
            new ConfigurationCommand(mMetaClient, System.out);
        configurationCommand.run();
        break;
      default:
        break;
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
        + "all categories of errors/warnings will be printed out.\n"
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
