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
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Sync direct children next time command.
 */
@ThreadSafe
@PublicApi
public final class SyncDirNextTimeCommand extends AbstractFileSystemCommand {

  private boolean mSyncNextTime;

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public SyncDirNextTimeCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "syncDirNextTime";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 2);
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    FileSystemCommandUtils.setDirectChildrenLoaded(mFileSystem, path, mSyncNextTime);
    System.out.format("Successfully marked the dir %s to %s%n", path,
        mSyncNextTime ? "trigger metadata sync on next access"
            : "skip metadata sync on next access");
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    mSyncNextTime = Boolean.parseBoolean(args[0]);
    runWildCardCmd(new AlluxioURI(args[1]), cl);
    return 0;
  }

  @Override
  public String getUsage() {
    return "syncDirNextTime <true|false> <path>\n"
        + "\ttrue means the next access will trigger a metadata sync on the dir"
        + "\tfalse means the next metadata sync is disabled";
  }

  @Override
  public String getDescription() {
    return "Marks a directory to either trigger a metadata sync or skip the "
        + "metadata sync on next access.";
  }
}
