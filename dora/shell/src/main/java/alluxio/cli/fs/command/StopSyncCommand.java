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

/**
 * This class represents a stop sync command.
 */
@PublicApi
public class StopSyncCommand extends AbstractFileSystemCommand {
  /**
   * Create a StopSync Command object.
   *
   * @param fsContext the filesystem of Alluxio
   */
  public StopSyncCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "stopSync";
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    mFileSystem.stopSync(path);
    System.out.println("Stopped automatic syncing of '" + path + "'.");
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
    return "stopSync <path>";
  }

  @Override
  public String getDescription() {
    return "Stops the automatic syncing process of the specified path). ";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }
}
