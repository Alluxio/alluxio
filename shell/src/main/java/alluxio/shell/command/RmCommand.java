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
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.DeleteOptions;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Removes the file specified by argv.
 */
@ThreadSafe
public final class RmCommand extends WithWildCardPathCommand {

  /**
   * @param conf the configuration for Alluxio
   * @param fs the filesystem of Alluxio
   */
  public RmCommand(Configuration conf, FileSystem fs) {
    super(conf, fs);
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
  protected Options getOptions() {
    return new Options().addOption(RECURSIVE_OPTION);
  }

  @Override
  void runCommand(AlluxioURI path, CommandLine cl) throws IOException {
    // TODO(calvin): Remove explicit state checking.
    try {
      boolean recursive = cl.hasOption("R");
      if (!mFileSystem.exists(path)) {
        throw new IOException("Path " + path + " does not exist");
      }
      if (!recursive && mFileSystem.getStatus(path).isFolder()) {
        throw new IOException("rm: cannot remove a directory, please try rm -R <path>");
      }

      DeleteOptions options = DeleteOptions.defaults().setRecursive(recursive);
      mFileSystem.delete(path, options);
      System.out.println(path + " has been removed");
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public String getUsage() {
    return "rm [-R] <path>";
  }

  @Override
  public String getDescription() {
    return "Removes the specified file. Specify -R to remove file or directory recursively.";
  }
}
