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

import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Gets the files that are being async persisted on worker.
 */
@ThreadSafe
public class GetFilesPersistInProgressCommand extends AbstractShellCommand {

  /**
   * Constructs a new instance of the command.
   *
   * @param fs the filesystem of Alluxio
   */
  public GetFilesPersistInProgressCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "getFilesPersistInProgress";
  }

  @Override
  protected int getNumOfArgs() {
    return 0;
  }

  @Override
  public void run(CommandLine cl) throws IOException, AlluxioException {
    List<String> filePaths = mFileSystem.getFilesPersistInProgress();
    System.out.println("Files that are currently persisted in progress on worker: ");
    for (String path : filePaths) {
      System.out.println(path);
    }
  }

  @Override
  public String getUsage() {
    return "getFilesPersistInProgressCommand";
  }

  @Override
  public String getDescription() {
    return "Gets the path of the files that are being async persisted on worker.";
  }

}
