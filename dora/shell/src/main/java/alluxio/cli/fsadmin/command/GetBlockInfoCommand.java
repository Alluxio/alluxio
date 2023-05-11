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
import alluxio.master.block.BlockId;
import alluxio.wire.BlockInfo;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

/**
 * Command for getting information from a block id.
 */
@PublicApi
public class GetBlockInfoCommand extends AbstractFsAdminCommand {
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
  public GetBlockInfoCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mConf = alluxioConf;
  }

  @Override
  public String getCommandName() {
    return "getBlockInfo";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    if (cl.hasOption(HELP_OPTION_NAME)) {
      System.out.println(getUsage());
      System.out.println(getDescription());
      return 0;
    }

    FileSystemAdminShellUtils.checkMasterClientService(mConf);

    long blockId;
    String arg = cl.getArgs()[0];
    try {
      blockId = Long.parseLong(arg);
    } catch (NumberFormatException e) {
      throw new InvalidArgumentException(arg + " is not a valid block id.");
    }

    BlockInfo info = null;
    try {
      info = mBlockClient.getBlockInfo(blockId);
    } catch (Exception e) {
      // ignore
    }
    long fileId = BlockId.getFileId(blockId);
    String path = null;
    try {
      path = mFsClient.getFilePath(fileId);
    } catch (Exception e) {
      // ignore
    }
    if (info != null) {
      System.out.println(info);
    } else {
      System.out.println("BlockMeta is not available for blockId: " + blockId);
    }
    if (path != null) {
      System.out.printf("This block belongs to file {id=%s, path=%s}%n", fileId, path);
    } else {
      System.out.printf("This block belongs to file {id=%s}%n", fileId);
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "getBlockInfo [blockId]";
  }

  @Override
  public String getDescription() {
    return "get the block information and file path of a specified block id.";
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
