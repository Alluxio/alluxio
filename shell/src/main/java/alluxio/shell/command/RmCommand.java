/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
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
