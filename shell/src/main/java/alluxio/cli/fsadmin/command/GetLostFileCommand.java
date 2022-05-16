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
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.LostBlockList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Map;

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

    Map<Long, LostBlockList> lostFiles = mFsClient.getLostFiles();
    if (lostFiles != null) {
      lostFiles.entrySet().stream().map((entry) -> {
        try {
          return new Pair(mFsClient.getFilePath(entry.getKey()), entry.getValue());
        } catch (AlluxioStatusException e) {
          return e.getMessage() + " for lost fileId " + entry.getKey();
        }
      }).sorted().forEach(System.out::println);
      System.out.println("Number of lost files is " + lostFiles.size());
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "getLostFile";
  }

  @Override
  public String getDescription() {
    return "Get all lost files.";
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(HELP_OPTION);
  }
}
