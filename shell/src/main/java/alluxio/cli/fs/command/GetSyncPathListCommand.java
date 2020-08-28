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
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.wire.SyncPointInfo;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.List;

/**
 * This class represents a getSyncPathList Command.
 */
@PublicApi
public class GetSyncPathListCommand extends AbstractFileSystemCommand{
  /**
   * Create a GetSyncPathListCommand object.
   *
   * @param fsContext file system
   */
  public GetSyncPathListCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "getSyncPathList";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    List<SyncPointInfo> files = mFileSystem.getSyncPathList();
    System.out.println("The following paths are under active sync");
    for (SyncPointInfo syncPointInfo : files) {
      System.out.print(syncPointInfo.getSyncPointUri() + "\t");
      switch (syncPointInfo.getSyncStatus()) {
        case NOT_INITIALLY_SYNCED:
          System.out.println("Initial Sync Skipped");
          break;
        case INITIALLY_SYNCED:
          System.out.println("Initial Sync Done");
          break;
        case SYNCING:
          System.out.println("Initial Sync In Progress");
          break;
        default:
          System.out.println("Invalid Syncing Status");
      }
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "getSyncPathList";
  }

  @Override
  public String getDescription() {
    return "Gets all the paths that are under active syncing right now.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 0);
  }
}
