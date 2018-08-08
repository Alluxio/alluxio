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
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Pins the given file or folder (recursively pinning all children if a folder). Pinned files are
 * never evicted from memory.
 */
@ThreadSafe
public final class PinCommand extends AbstractFileSystemCommand {

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
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    FileSystemCommandUtils.setPinned(mFileSystem, path, true);
    System.out.println("File '" + path + "' was successfully pinned.");
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    if (args.length == 0) {
      List<String> pinnedFiles = mFileSystem.getPinnedFiles();
      if (pinnedFiles.size() > 0) {
        System.out.println("The current pinned file paths are listed below: ");
      } else {
        System.out.println("There are no pinned file paths yet, please use 'pin <path>'.");
      }
      Collections.sort(pinnedFiles);
      for (String pinnedFile : pinnedFiles) {
        System.out.println(pinnedFile);
      }
      return 0;
    }
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);
    return 0;
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
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    if (cl.getArgs().length != 1 && cl.getArgs().length != 0) {
      throw new InvalidArgumentException("Command pin takes 0 or 1 arguments, not " + cl
          .getArgs().length);
    }
  }
}
