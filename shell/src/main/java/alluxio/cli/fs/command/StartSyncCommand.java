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

import com.google.common.base.Throwables;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Start a polling sync of a particular path.
 */

@ThreadSafe
@PublicApi
public final class StartSyncCommand extends AbstractFileSystemCommand {

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public StartSyncCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "startSync";
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl) {
    System.out.println("Starting a full sync of '" + path
        + "'. You can check the status of the sync using getSyncPathList cmd");
    try {
      mFileSystem.startSync(path);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e);
      System.out.println("Failed to start automatic syncing of '" + path + "'.");
      throw new RuntimeException(e);
    }
    System.out.println("Started automatic syncing of '" + path + "'.");
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
    return "startSync <path>";
  }

  @Override
  public String getDescription() {
    return "Starts the automatic syncing process of the specified path). ";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }
}
