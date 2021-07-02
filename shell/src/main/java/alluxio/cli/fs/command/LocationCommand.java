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

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.wire.BlockLocation;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays a list of hosts that have the file specified in args stored.
 */
@ThreadSafe
@PublicApi
public final class LocationCommand extends AbstractFileSystemCommand {
  /**
   * Constructs a new instance to display a list of hosts that have the file specified in args
   * stored.
   *
   * @param fsContext the filesystem of Alluxio
   */
  public LocationCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "location";
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(plainPath);

    System.out.println(plainPath + " with file id " + status.getFileId() + " is on nodes: ");
    AlluxioBlockStore blockStore = AlluxioBlockStore.create(mFsContext);
    for (long blockId : status.getBlockIds()) {
      for (BlockLocation location : blockStore.getInfo(blockId).getLocations()) {
        System.out.println(location.getWorkerAddress().getHost());
      }
    }
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);
    return 0;
  }

  @Override
  public String getUsage() {
    return "location <path>";
  }

  @Override
  public String getDescription() {
    return "Displays the list of hosts storing the specified file.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }
}
