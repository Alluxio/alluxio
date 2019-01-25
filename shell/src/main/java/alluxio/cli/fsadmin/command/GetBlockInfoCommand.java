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
import alluxio.exception.status.InvalidArgumentException;
import alluxio.master.block.BlockId;
import alluxio.wire.BlockInfo;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

/**
 * Command for getting block information from block id.
 */
public class GetBlockInfoCommand extends AbstractFsAdminCommand {
  private static final String FILE_INFO_PATTERN = "FileId=%s, FilePath=%s%n";
  /**
   * @param context fsadmin command context
   */
  public GetBlockInfoCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "getBlockInfo";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    long blockId;
    try {
      blockId = Long.parseLong(cl.getArgs()[0]);
    } catch (NumberFormatException e) {
      throw new InvalidArgumentException("Did not provide valid block id");
    }
    BlockInfo info = mBlockClient.getBlockInfo(blockId);
    long fileId = BlockId.createBlockId(BlockId.getContainerId(blockId),
        BlockId.getMaxSequenceNumber());
    String path = mFsClient.getFilePath(fileId);
    System.out.println(info);
    System.out.printf(FILE_INFO_PATTERN, fileId, path);
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
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoMoreThan(this, cl, 1);
  }
}
