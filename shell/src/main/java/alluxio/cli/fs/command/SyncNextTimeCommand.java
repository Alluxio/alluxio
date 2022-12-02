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
public final class SyncNextTimeCommand extends AbstractFileSystemCommand {

  private boolean mLoaded;

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public SyncNextTimeCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "syncNextTime";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 2);
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    FileSystemCommandUtils.setDirectChildrenLoaded(mFileSystem, path, mLoaded);
    System.out.println("Path '" + path + "' was successfully set sync direct children to "
        + mLoaded);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    mLoaded = Boolean.parseBoolean(args[0]);
    runWildCardCmd(new AlluxioURI(args[1]), cl);
    return 0;
  }

  @Override
  public String getUsage() {
    return "syncNextTime <true|false> <path>";
  }

  @Override
  public String getDescription() {
    return "Sets sync direct children next time for a specific directory to true or false";
  }
}
