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
import alluxio.client.file.options.FreeOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Frees the given file or folder from Alluxio storage (recursively freeing all children if a
 * folder).
 */
@ThreadSafe
public final class FreeCommand extends AbstractFileSystemCommand {

  private static final Option FORCE_OPTION =
      Option.builder("f")
          .required(false)
          .hasArg(false)
          .desc("force to free files even pinned")
          .build();
  private boolean mIsForced;

  /**
   * Constructs a new instance to free the given file or folder from Alluxio.
   *
   * @param fs the filesystem of Alluxio
   */
  public FreeCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(FORCE_OPTION);
  }

  @Override
  public String getCommandName() {
    return "free";
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    FreeOptions options = FreeOptions.defaults().setRecursive(true).setForced(mIsForced);
    mFileSystem.free(path, options);
    System.out.println(path + " was successfully freed from memory.");
  }

  @Override
  public String getUsage() {
    return "free [-f] <path>";
  }

  @Override
  public String getDescription() {
    return "Frees the space occupied by a file or a directory in Alluxio."
        + " Specify -f to force freeing pinned files in the directory.";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    mIsForced = cl.hasOption("f");
    runWildCardCmd(path, cl);

    return 0;
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }
}
