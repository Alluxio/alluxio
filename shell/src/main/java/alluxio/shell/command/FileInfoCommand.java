/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays the file's all blocks info.
 */
@ThreadSafe
public final class FileInfoCommand extends WithWildCardPathCommand {

  /**
   * @param conf the configuration for Alluxio
   * @param fs the filesystem of Alluxio
   */
  public FileInfoCommand(Configuration conf, FileSystem fs) {
    super(conf, fs);
  }

  @Override
  public String getCommandName() {
    return "fileInfo";
  }

  @Override
  void runCommand(AlluxioURI path, CommandLine cl) throws IOException {
    URIStatus status;
    try {
      status = mFileSystem.getStatus(path);
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }

    if (status.isFolder()) {
      throw new IOException(path + " is a directory path so does not have file blocks.");
    }

    System.out.println(status);
    System.out.println("Containing the following blocks: ");
    for (long blockId : status.getBlockIds()) {
      System.out.println(AlluxioBlockStore.get().getInfo(blockId));
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
