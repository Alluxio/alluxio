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
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.wire.FileInfo;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

/**
 * Command for getting file path from a file id.
 */
@PublicApi
public class GetFilePathCommand extends AbstractFsAdminCommand {
  private static final String HELP_OPTION_NAME = "h";
  private static final Option HELP_OPTION =
      Option.builder(HELP_OPTION_NAME)
          .required(false)
          .hasArg(false)
          .desc("print help information.")
          .build();

  private final AlluxioConfiguration mConf;

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public GetFilePathCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mConf = alluxioConf;
  }

  @Override
  public String getCommandName() {
    return "getFilePath";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    if (cl.hasOption(HELP_OPTION_NAME)) {
      System.out.println(getUsage());
      System.out.println(getDescription());
      return 0;
    }

    FileSystemAdminShellUtils.checkMasterClientService(mConf);

    long fileId;
    String arg = cl.getArgs()[0];
    try {
      fileId = Long.parseLong(arg);
    } catch (NumberFormatException e) {
      throw new InvalidArgumentException(arg + " is not a valid file id.");
    }

    try {
      FileInfo fileInfo = mFsClient.getFilePath(fileId);
      System.out.println(fileInfo);
    } catch (Exception e) {
      System.err.println("Command failed with exception:");
      e.printStackTrace(System.err);
      return -1;
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "getFilePath [fileId]";
  }

  @Override
  public String getDescription() {
    return "get the file path of a specified file id.";
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(HELP_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }
}
