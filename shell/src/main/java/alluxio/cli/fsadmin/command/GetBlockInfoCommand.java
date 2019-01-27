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
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.master.block.BlockId;
import alluxio.wire.BlockInfo;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Command for getting block information from block id.
 */
public class GetBlockInfoCommand extends AbstractFsAdminCommand {
  private static final String HEADER_PATTERN = "Showing information of block %s:%n";
  private static final String HELP_OPTION_NAME = "h";
  @VisibleForTesting
  public static final String INVALID_BLOCK_ID_INFO = "%s is not a valid block id%n%n";
  private static final String FILE_INFO_PATTERN = "This block belongs to file {id=%s, path=%s}%n%n";

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

    List<Long> blockIds = getBlockIds(cl.getArgs()[0]);
    for (Long id : blockIds) {
      getAndPrintBlockInfo(id);
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "getBlockInfo [blockId]";
  }

  @Override
  public String getDescription() {
    return "get block information and file paths of block ids separated by comma.";
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

  /**
   * Gets block ids from input string.
   *
   * @param input an input string
   * @return the block ids
   */
  private List<Long> getBlockIds(String input) {
    return Arrays.stream(input.split(",")).map(a -> {
      try {
        return Long.parseLong(a);
      } catch (NumberFormatException e) {
        System.out.printf(INVALID_BLOCK_ID_INFO, a);
        return -1L;
      }
    }).filter(a -> a != -1L).collect(Collectors.toList());
  }

  /**
   * Gets and prints the information of a block id.
   *
   * @param blockId a block id
   */
  private void getAndPrintBlockInfo(long blockId) throws IOException {
    BlockInfo info;
    try {
      info = mBlockClient.getBlockInfo(blockId);
    } catch (Exception e) {
      // Don't error out when one block id is invalid
      System.out.println(e.getMessage());
      return;
    }
    System.out.printf(HEADER_PATTERN, blockId);
    long fileId = BlockId.createBlockId(BlockId.getContainerId(blockId),
        BlockId.getMaxSequenceNumber());
    String path = mFsClient.getFilePath(fileId);
    System.out.println(info);
    System.out.printf(FILE_INFO_PATTERN, fileId, path);
  }
}
