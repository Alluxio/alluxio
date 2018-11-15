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
import alluxio.client.file.FileSystemClientOptions;
import alluxio.client.file.FileSystemUtils;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.DeletePOptions;

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
public class CheckConsistencyCommand extends AbstractFileSystemCommand {

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
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    checkConsistency(plainPath, cl.hasOption("r"));
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
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

    runWildCardCmd(root, cl);
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
    List<AlluxioURI> inconsistentUris = FileSystemUtils.checkConsistency(path,
        FileSystemClientOptions.getCheckConsistencyOptions());
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
      for (AlluxioURI inconsistentUri : inconsistentUris) {
        URIStatus status = mFileSystem.getStatus(inconsistentUri);
        if (status.isFolder()) {
          inconsistentDirs.add(inconsistentUri);
          continue;
        }
        System.out.println("repairing path: " + inconsistentUri);
        DeletePOptions deleteOptions =
            FileSystemClientOptions.getDeleteOptions().toBuilder().setAlluxioOnly(true).build();
        mFileSystem.delete(inconsistentUri, deleteOptions);
        mFileSystem.exists(inconsistentUri);
        System.out.println(inconsistentUri + " repaired");
        System.out.println();
      }
      for (AlluxioURI uri : inconsistentDirs) {
        DeletePOptions deleteOptions = FileSystemClientOptions.getDeleteOptions().toBuilder()
            .setAlluxioOnly(true).setRecursive(true).build();
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
