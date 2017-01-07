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

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays the path's info.
 * If path is a directory it displays the directory's info.
 * If path is a file, it displays the file's all blocks info.
 */
@ThreadSafe
public final class FileInfoCommand extends WithWildCardPathCommand {
  /**
   * @param fs the filesystem of Alluxio
   */
  public FileInfoCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "stat";
  }

  @Override
  protected void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(path);
    if (status.isFolder()) {
      System.out.println(path + " is a directory path.");
      System.out.println(status);
    } else {
      System.out.println(path + " is a file path.");
      System.out.println(status);
      System.out.println("Containing the following blocks: ");
      AlluxioBlockStore blockStore = AlluxioBlockStore.create();
      for (long blockId : status.getBlockIds()) {
        System.out.println(blockStore.getInfo(blockId));
      }
    }
  }

  @Override
  public String getUsage() {
    return "stat <path>";
  }

  @Override
  public String getDescription() {
    return "Displays info for the specified path both file and directory.";
  }
}
