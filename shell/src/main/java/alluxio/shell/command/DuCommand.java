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
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays the size of a file or a directory specified by argv.
 */
@ThreadSafe
public final class DuCommand extends WithWildCardPathCommand {

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
  protected void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {
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
    List<URIStatus> statuses = fs.listStatus(path);
    for (URIStatus status : statuses) {
      if (status.isFolder()) {
        AlluxioURI subFolder = new AlluxioURI(status.getPath());
        sizeInBytes += getFileOrFolderSize(fs, subFolder);
      } else {
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
}
