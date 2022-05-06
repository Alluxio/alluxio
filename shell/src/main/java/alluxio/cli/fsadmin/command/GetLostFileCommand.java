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
import alluxio.cli.fsadmin.FileSystemAdminShellUtils;
import alluxio.conf.AlluxioConfiguration;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;

/**
 * Command for getting lost files.
 */
@PublicApi
public class GetLostFileCommand extends AbstractFsAdminCommand {
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
  public GetLostFileCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mConf = alluxioConf;
  }

  @Override
  public String getCommandName() {
    return "getLostFile";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    if (cl.hasOption(HELP_OPTION_NAME)) {
      System.out.println(getUsage());
      System.out.println(getDescription());
      return 0;
    }

    FileSystemAdminShellUtils.checkMasterClientService(mConf);

    List<Long> lostFiles = mFsClient.getLostFiles();
    if (lostFiles != null) {
      lostFiles.forEach(System.out::println);
      System.out.println("Size of lostFile is " + lostFiles.size());
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "getLostFile";
  }

  @Override
  public String getDescription() {
    return "get the lost file.";
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(HELP_OPTION);
  }
}
