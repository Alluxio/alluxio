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
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays the size of a file or a directory specified by argv.
 */
@ThreadSafe
public final class DuCommand extends AbstractFileSystemCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public DuCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "du";
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    long sizeInBytes = getFileOrFolderSize(mFileSystem, path);
    System.out.println(path + " is " + sizeInBytes + " bytes");
  }

  /**
   * Calculates the size of a path (file or folder) specified by a {@link AlluxioURI}.
   *
   * @param fs a {@link FileSystem}
   * @param path a {@link AlluxioURI} denoting the path
   * @return total size of the specified path in byte
   */
  private long getFileOrFolderSize(FileSystem fs, AlluxioURI path)
      throws AlluxioException, IOException {
    long sizeInBytes = 0;
    ListStatusOptions listOptions = ListStatusOptions.defaults().setRecursive(true);
    List<URIStatus> statuses = fs.listStatus(path, listOptions);
    for (URIStatus status : statuses) {
      if (!status.isFolder()) {
        sizeInBytes += status.getLength();
      }
    }
    return sizeInBytes;
  }

  @Override
  public String getUsage() {
    return "du <path>";
  }

  @Override
  public String getDescription() {
    return "Displays the size of the specified file or directory.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);

    return 0;
  }
}
