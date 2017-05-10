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
import alluxio.client.file.options.DeleteOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;

/**
 * Removes the file specified by argv.
 */
@ThreadSafe
public final class RmCommand extends WithWildCardPathCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public RmCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "rm";
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(RECURSIVE_OPTION).addOption(DELETE_ALLUXIO_ONLY);
  }

  @Override
  protected void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {
    // TODO(calvin): Remove explicit state checking.
    boolean recursive = cl.hasOption("R");
    if (!mFileSystem.exists(path)) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    }
    if (!recursive && mFileSystem.getStatus(path).isFolder()) {
      throw new IOException(
          path.getPath() + " is a directory, to remove it, please use \"rm -R <path>\"");
    }

    boolean isAlluxioOnly = cl.hasOption("alluxioOnly");
    //System.out.println("isAlluxioOnly: " + isAlluxioOnly);
    DeleteOptions options = DeleteOptions.defaults().setRecursive(recursive).setAlluxioOnly(isAlluxioOnly);
    mFileSystem.delete(path, options);
    if (!isAlluxioOnly) {
      System.out.println(path + " has been removed in Alluxion space and Ufs space");
    }else {
      System.out.println(path + " just been removed in Alluxio Space, but Ufs space still exists");
    }
  }

  @Override
  public String getUsage() {
    return "rm [-R] [-alluxioOnly] <path>";
  }

  @Override
  public String getDescription() {
    return "Removes the specified file. Specify -R to remove file or directory recursively." +
        "Specify -alluxioOnly just remove block data and metadata in Alluxio space";
  }
}
