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

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.wire.BlockLocation;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays a list of hosts that have the file specified in args stored.
 */
@ThreadSafe
public final class LocationCommand extends WithWildCardPathCommand {
  /**
   * Constructs a new instance to display a list of hosts that have the file specified in args
   * stored.
   *
   * @param fs the filesystem of Alluxio
   */
  public LocationCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "location";
  }

  @Override
  protected void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(path);

    System.out.println(path + " with file id " + status.getFileId() + " is on nodes: ");
    AlluxioBlockStore blockStore = AlluxioBlockStore.create();
    for (long blockId : status.getBlockIds()) {
      for (BlockLocation location : blockStore.getInfo(blockId).getLocations()) {
        System.out.println(location.getWorkerAddress().getHost());
      }
    }
  }

  @Override
  public String getUsage() {
    return "location <path>";
  }

  @Override
  public String getDescription() {
    return "Displays the list of hosts storing the specified file.";
  }
}
