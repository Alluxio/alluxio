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
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Unpins the given file or folder (recursively unpinning all children if a folder). Pinned files
 * are never evicted from memory, so this method will allow such files to be evicted.
 */
@ThreadSafe
public final class UnpinCommand extends WithWildCardPathCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public UnpinCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "unpin";
  }

  @Override
  protected void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {
    CommandUtils.setPinned(mFileSystem, path, false);
    System.out.println("File '" + path + "' was successfully unpinned.");
  }

  @Override
  public String getUsage() {
    return "unpin <path>";
  }

  @Override
  public String getDescription() {
    return "Unpins the given file or folder from memory (works recursively for a directory).";
  }

  @Override
  public boolean validateArgs(String... args) {
    return args.length >= 1;
  }
}
