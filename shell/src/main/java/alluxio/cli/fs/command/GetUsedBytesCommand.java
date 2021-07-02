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

package alluxio.cli.fs.command;

import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Gets number of bytes used in the {@link FileSystem}.
 */
@ThreadSafe
@PublicApi
public final class GetUsedBytesCommand extends AbstractFileSystemCommand {
  /**
   * Constructs a new instance to get the number of bytes used in the {@link FileSystem}.
   *
   * @param fsContext the filesystem of Alluxio
   */
  public GetUsedBytesCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "getUsedBytes";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 0);
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    AlluxioBlockStore blockStore = AlluxioBlockStore.create(mFsContext);
    long usedBytes = blockStore.getUsedBytes();
    System.out.println("Used Bytes: " + usedBytes);
    return 0;
  }

  @Override
  public String getUsage() {
    return "getUsedBytes";
  }

  @Override
  public String getDescription() {
    return "Gets number of bytes used in the Alluxio file system.";
  }
}
