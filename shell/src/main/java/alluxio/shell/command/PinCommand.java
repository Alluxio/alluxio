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
 * Pins the given file or folder (recursively pinning all children if a folder). Pinned files are
 * never evicted from memory.
 */
@ThreadSafe
public final class PinCommand extends WithWildCardPathCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public PinCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "pin";
  }

  @Override
  protected void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {
    CommandUtils.setPinned(mFileSystem, path, true);
    System.out.println("File '" + path + "' was successfully pinned.");
  }

  @Override
  public String getUsage() {
    return "pin <path>";
  }

  @Override
  public String getDescription() {
    return "Pins the given file or directory in memory (works recursively for directories). "
      + "Pinned files are never evicted from memory, unless TTL is set.";
  }

  @Override
  public boolean validateArgs(String... args) {
    return args.length >= 1;
  }
}
