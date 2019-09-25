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
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Pins the given file or folder (recursively pinning all children if a folder). Pinned files are
 * never evicted from memory.
 */
@ThreadSafe
@PublicApi
public final class PinCommand extends AbstractFileSystemCommand {

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public PinCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "pin";
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    // args[0] is the path, args[1] to args[end] is the list of possible media to pin
    List<String> pinnedMediumTypes = Arrays.asList(Arrays.copyOfRange(args, 1, args.length));
    List<String> availableMediumList = mFsContext.getPathConf(path).getList(
        PropertyKey.MASTER_TIERED_STORE_GLOBAL_MEDIUMTYPE, ",");
    List<String> invalidMediumType = new ArrayList<>();
    List<String> validMediumType = new ArrayList<>();
    for (String mediumType: pinnedMediumTypes) {
      if (availableMediumList.contains(mediumType)) {
        validMediumType.add(mediumType);
      } else {
        invalidMediumType.add(mediumType);
      }
    }
    if (!invalidMediumType.isEmpty()) {
      throw new IllegalArgumentException("Invalid medium to pin the file. "
          + String.join(",", invalidMediumType) + " are invalid. "
          + String.join(",", availableMediumList) + " are valid medium types");
    }
    FileSystemCommandUtils.setPinned(mFileSystem, path, true, pinnedMediumTypes);
    System.out.println("File '" + path + "' was successfully pinned.");
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);

    runWildCardCmd(path, cl);
    return 0;
  }

  @Override
  public String getUsage() {
    return "pin <path> media1 media2 media3 ...";
  }

  @Override
  public String getDescription() {
    return "Pins the given file or directory in memory (works recursively for directories). "
      + "Pinned files are never evicted from memory, unless TTL is set.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }
}
