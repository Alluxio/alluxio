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
import alluxio.client.file.FileSystemUtils;
import alluxio.client.file.options.CheckConsistencyOptions;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Command for checking the consistency of a file or folder between Alluxio and the under storage.
 */
public class CheckConsistencyCommand extends AbstractShellCommand {
  /**
   * @param fs the filesystem of Alluxio
   */
  public CheckConsistencyCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public String getCommandName() {
    return "checkConsistency";
  }

  @Override
  public void run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI root = new AlluxioURI(args[0]);
    CheckConsistencyOptions options = CheckConsistencyOptions.defaults();
    List<AlluxioURI> inconsistentUris = FileSystemUtils.checkConsistency(root, options);
    if (inconsistentUris.isEmpty()) {
      System.out.println(root + " is consistent with the under storage system.");
    } else {
      Collections.sort(inconsistentUris);
      System.out.println("The following files are inconsistent:");
      for (AlluxioURI uri : inconsistentUris) {
        System.out.println(uri);
      }
    }
  }

  @Override
  public String getUsage() {
    return "checkConsistency <Alluxio path>";
  }

  @Override
  public String getDescription() {
    return "Checks the consistency of a persisted file or directory in Alluxio. Any files or "
        + "directories which only exist in Alluxio or do not match the metadata of files in the "
        + "under storage will be returned. An administrator should then reconcile the differences.";
  }
}
