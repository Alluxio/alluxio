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
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays the path's info.
 * If path is a directory and with -l args, it displays the directory's
 * info and all direct children's info.
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
    return "fileInfo";
  }

  @Override
  protected Options getOptions() {
    return new Options()
        .addOption(LIST_ALL_CHILDREN_OPTION);
  }

  /**
   * @param status the URIStatus to be print
   * @throws IOException if an I/O error occurs
   */
  private void printFileInfo(URIStatus status) throws IOException {
    System.out.println(status);
    System.out.println("Containing the following blocks: ");
    AlluxioBlockStore blockStore = AlluxioBlockStore.create();
    for (long blockId : status.getBlockIds()) {
      System.out.println(blockStore.getInfo(blockId));
    }
  }

  /**
   * Displays information for the path with specified args.
   *
   * @param path The {@link AlluxioURI} path as the input of the command
   * @param listAllChildren Whether list all children of a directory
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void fileInfo(AlluxioURI path, boolean listAllChildren) throws AlluxioException,
      IOException {
    URIStatus status = mFileSystem.getStatus(path);
    if (status.isFolder() && listAllChildren) {
      List<URIStatus> childrens = mFileSystem.listStatus(path);
      System.out.println(path + " is a directory and have " + childrens.size() + " child");
      System.out.println("The directory's information as following: ");
      System.out.println(status);
      if (childrens.size() > 0) {
        System.out.println("The childrens' information as following: ");
        for (int i = 0; i < childrens.size(); i++) {
          System.out.println("");
          System.out.println("child " + (i + 1) + ": ");
          printFileInfo(childrens.get(i));
        }
      }
    } else {
      if (status.isFolder()) {
        System.out.println(path + " is a directory and the information as following: ");
        System.out.println(status);
      } else {
        System.out.println(path + " is a file and the information as following: ");
        printFileInfo(status);
      }
    }
  }

  @Override
  protected void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {
    fileInfo(path, cl.hasOption("l"));
  }

  @Override
  public String getUsage() {
    return "fileInfo [-l] <path>";
  }

  @Override
  public String getDescription() {
    return "Displays info for the specified path both file and directory."
        + " Specify -l to display directory's all children info.";
  }
}
