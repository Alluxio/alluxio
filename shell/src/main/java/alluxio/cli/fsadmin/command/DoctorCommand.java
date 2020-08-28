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

import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.cli.fsadmin.FileSystemAdminShellUtils;
import alluxio.cli.fsadmin.doctor.ConfigurationCommand;
import alluxio.cli.fsadmin.doctor.StorageCommand;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.InvalidArgumentException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

/**
 * Shows errors or warnings that users should pay attention to.
 */
@PublicApi
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
    STORAGE, // Show worker lost storage warnings
  }

  private final AlluxioConfiguration mConf;

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public DoctorCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mConf = alluxioConf;
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

    FileSystemAdminShellUtils.checkMasterClientService(mConf);

    // Get the doctor category
    Command command = Command.ALL;
    if (args.length == 1) {
      switch (args[0]) {
        case "configuration":
          command = Command.CONFIGURATION;
          break;
        case "storage" :
          command = Command.STORAGE;
          break;
        default:
          System.out.println(getUsage());
          System.out.println(getDescription());
          throw new InvalidArgumentException("doctor category is invalid.");
      }
    }

    if (command.equals(Command.CONFIGURATION) || command.equals(Command.ALL)) {
      ConfigurationCommand configurationCommand =
          new ConfigurationCommand(mMetaClient, System.out);
      configurationCommand.run();
    }

    if (command.equals(Command.STORAGE) || command.equals(Command.ALL)) {
      StorageCommand storageCommand = new StorageCommand(mBlockClient, System.out);
      storageCommand.run();
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return usage();
  }

  /**
   * @return the usage for the doctor command
   */
  @VisibleForTesting
  public static String usage() {
    return "doctor [category]";
  }

  @Override
  public String getDescription() {
    return description();
  }

  /**
   * @return the description for the doctor command
   */
  @VisibleForTesting
  public static String description() {
    return "Show Alluxio errors and warnings.\n"
        + "Where [category] is an optional argument. If no arguments are passed in, "
        + "all categories of errors/warnings will be printed out.\n"
        + "[category] can be one of the following:\n"
        + "    configuration    server-side configuration errors/warnings\n"
        + "    storage          worker lost storage warnings\n";
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
