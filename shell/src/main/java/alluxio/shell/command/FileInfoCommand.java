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
import alluxio.exception.InvalidPathException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays the file's all blocks info.
 */
@ThreadSafe
public final class FileInfoCommand extends WithWildCardPathCommand {

  /** The block store client. */
  private final AlluxioBlockStore mBlockStore;

  /**
   * @param fs the filesystem of Alluxio
   */
  public FileInfoCommand(FileSystem fs) {
    super(fs);
    mBlockStore = new AlluxioBlockStore();
  }

  @Override
  public String getCommandName() {
    return "fileInfo";
  }

  @Override
  void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(path);

    if (status.isFolder()) {
      throw new InvalidPathException(path + " is a directory path so does not have file blocks.");
    }

    System.out.println(status);
    System.out.println("Containing the following blocks: ");
    for (long blockId : status.getBlockIds()) {
      System.out.println(mBlockStore.getInfo(blockId));
    }
  }

  @Override
  public String getUsage() {
    return "fileInfo <path>";
  }

  @Override
  public String getDescription() {
    return "Displays all block info for the specified file.";
  }
}
