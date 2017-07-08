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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Removes the file specified by argv.
 */
@ThreadSafe
public final class RmCommand extends WithWildCardPathCommand {

  private static final Option RECURSIVE_OPTION =
      Option.builder("R")
          .required(false)
          .hasArg(false)
          .desc("delete files and subdirectories recursively")
          .build();

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
    return new Options().addOption(RECURSIVE_OPTION).addOption(REMOVE_UNCHECKED_OPTION);
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

    DeleteOptions options = DeleteOptions.defaults().setRecursive(recursive);
    if (cl.hasOption(REMOVE_UNCHECKED_OPTION_CHAR)) {
      options.setUnchecked(true);
    }
    mFileSystem.delete(path, options);
    System.out.println(path + " has been removed");
  }

  @Override
  public String getUsage() {
    return "rm [-R] <path>";
  }

  @Override
  public String getDescription() {
    return "Removes the specified file. Specify -R to remove file or directory recursively.";
  }

  @Override
  public boolean validateArgs(String... args) {
    return args.length >= 1;
  }
}
