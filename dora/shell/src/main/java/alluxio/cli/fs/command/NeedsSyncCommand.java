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
 * Marks a path in Alluxio as needing metadata synchronization with the UFS.
 * The next time the path or any child path is accessed a metadata sync for
 * that path will be performed.
 */
@ThreadSafe
@PublicApi
public class NeedsSyncCommand extends AbstractFileSystemCommand {

  /**
   * Constructs a new instance of a needsSync command for the given Alluxio path.
   *
   * @param fsContext the filesystem of Alluxio
   */
  public NeedsSyncCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "needsSync";
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    needsSync(plainPath);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);

    return 0;
  }

  private void needsSync(AlluxioURI path) throws IOException {
    try {
      mFileSystem.needsSync(path);
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public String getUsage() {
    return "needsSync <path>";
  }

  @Override
  public String getDescription() {
    return "Marks the path in Alluxio as needing synchronization with the under file system. "
        + "The next time the path or any of its children are accessed a metadata synchronization"
        + " will be performed.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }
}
