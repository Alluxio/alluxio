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
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.DeletePOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Removes the file specified by argv.
 */
@ThreadSafe
@PublicApi
public final class RmCommand extends AbstractFileSystemCommand {

  private static final Option RECURSIVE_OPTION =
      Option.builder("R")
          .required(false)
          .hasArg(false)
          .desc("delete files and subdirectories recursively")
          .build();

  private static final String REMOVE_UNCHECKED_OPTION_CHAR = "U";
  private static final Option REMOVE_UNCHECKED_OPTION =
      Option.builder(REMOVE_UNCHECKED_OPTION_CHAR)
          .required(false)
          .hasArg(false)
          .desc("remove directories without checking UFS contents are in sync")
          .build();

  private static final Option REMOVE_ALLUXIO_ONLY =
      Option.builder()
          .longOpt("alluxioOnly")
          .required(false)
          .hasArg(false)
          .desc("remove data and metadata from Alluxio space only")
          .build();

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public RmCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "rm";
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(RECURSIVE_OPTION)
        .addOption(REMOVE_UNCHECKED_OPTION)
        .addOption(REMOVE_ALLUXIO_ONLY);
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    // TODO(calvin): Remove explicit state checking.
    boolean recursive = cl.hasOption("R");
    if (!mFileSystem.exists(path)) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    }
    if (!recursive && mFileSystem.getStatus(path).isFolder()) {
      throw new IOException(
          path.getPath() + " is a directory, to remove it, please use \"rm -R <path>\"");
    }
    boolean isAlluxioOnly = cl.hasOption(REMOVE_ALLUXIO_ONLY.getLongOpt());
    DeletePOptions options =
        DeletePOptions.newBuilder().setRecursive(recursive).setAlluxioOnly(isAlluxioOnly)
            .setUnchecked(cl.hasOption(REMOVE_UNCHECKED_OPTION_CHAR)).build();

    mFileSystem.delete(path, options);
    if (!isAlluxioOnly) {
      System.out.println(path + " has been removed");
    } else {
      System.out.println(path + " has been removed from Alluxio space");
    }
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
    return "rm [-R] [-U] [--alluxioOnly] <path>";
  }

  @Override
  public String getDescription() {
    return "Removes the specified file. Specify -R to remove file or directory recursively."
        + " Specify -U to remove directories without checking UFS contents are in sync."
        + " Specify -alluxioOnly to remove data and metadata from alluxio space only.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }
}
