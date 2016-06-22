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
import alluxio.client.file.options.FreeOptions;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Frees the given file or folder from Alluxio in-memory (recursively freeing all children if a
 * folder).
 */
@ThreadSafe
public final class FreeCommand extends WithWildCardPathCommand {

  /**
   * Constructs a new instance to free the given file or folder from Alluxio.
   *
   * @param fs the filesystem of Alluxio
   */
  public FreeCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "free";
  }

  @Override
  void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {
    FreeOptions options = FreeOptions.defaults().setRecursive(true);
    mFileSystem.free(path, options);
    System.out.println(path + " was successfully freed from memory.");
  }

  @Override
  public String getUsage() {
    return "free <path>";
  }

  @Override
  public String getDescription() {
    return "Frees the space occupied by a file or a directory in Alluxio.";
  }
}
