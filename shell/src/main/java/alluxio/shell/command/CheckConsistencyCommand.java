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
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CheckConsistencyOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Command for checking the consistency of a file or folder between Alluxio and the under storage.
 */
public class CheckConsistencyCommand extends AbstractShellCommand {

  private static final Option REPAIR_OPTION =
      Option.builder("r")
          .required(false)
          .hasArg(false)
          .desc("repair inconsistent files")
          .build();

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
  public Options getOptions() {
    return new Options().addOption(REPAIR_OPTION);
  }

  @Override
  public String getCommandName() {
    return "checkConsistency";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI root = new AlluxioURI(args[0]);
    checkConsistency(root, cl.hasOption("r"));
    return 0;
  }

  /**
   * Checks the inconsistent files and directories which exist in Alluxio but don't exist in the
   * under storage, repairs the inconsistent paths by deleting them if repairConsistency is true.
   *
   * @param path the specified path to be checked
   * @param repairConsistency whether to repair the consistency or not
   * @throws AlluxioException
   * @throws IOException
   */
  private void checkConsistency(AlluxioURI path, boolean repairConsistency) throws
      AlluxioException, IOException {
    CheckConsistencyOptions options = CheckConsistencyOptions.defaults();
    List<AlluxioURI> inconsistentUris = FileSystemUtils.checkConsistency(path, options);
    if (inconsistentUris.isEmpty()) {
      System.out.println(path + " is consistent with the under storage system.");
      return;
    }
    if (!repairConsistency) {
      Collections.sort(inconsistentUris);
      System.out.println("The following files are inconsistent:");
      for (AlluxioURI uri : inconsistentUris) {
        System.out.println(uri);
      }
    } else {
      Collections.sort(inconsistentUris);
      System.out.println(path + " has: " + inconsistentUris.size() + " inconsistent files.");
      List<AlluxioURI> inconsistentDirs = new ArrayList<AlluxioURI>();
      for (int i = 0; i < inconsistentUris.size(); i++) {
        AlluxioURI inconsistentUri = inconsistentUris.get(i);
        URIStatus status = mFileSystem.getStatus(inconsistentUri);
        if (status.isFolder()) {
          inconsistentDirs.add(inconsistentUri);
          continue;
        }
        System.out.println("repairing path: " + inconsistentUri);
        DeleteOptions deleteOptions = DeleteOptions.defaults().setAlluxioOnly(true);
        mFileSystem.delete(inconsistentUri, deleteOptions);
        mFileSystem.exists(inconsistentUri);
        System.out.println(inconsistentUri + " repaired");
        System.out.println();
      }
      for (AlluxioURI uri : inconsistentDirs) {
        DeleteOptions deleteOptions = DeleteOptions.defaults().setAlluxioOnly(true)
            .setRecursive(true);
        System.out.println("repairing path: " + uri);
        mFileSystem.delete(uri, deleteOptions);
        mFileSystem.exists(uri);
        System.out.println(uri + "repaired");
        System.out.println();
      }
    }
  }

  @Override
  public String getUsage() {
    return "checkConsistency [-r] <Alluxio path>";
  }

  @Override
  public String getDescription() {
    return "Checks the consistency of a persisted file or directory in Alluxio. Any files or "
        + "directories which only exist in Alluxio or do not match the metadata of files in the "
        + "under storage will be returned. An administrator should then reconcile the differences."
        + "Specify -r to repair the inconsistent files.";
  }
}
