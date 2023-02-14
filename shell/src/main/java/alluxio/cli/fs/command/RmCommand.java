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
      Option.builder("R").longOpt("recursive")
          .required(false)
          .hasArg(false)
          .desc("delete files and subdirectories recursively")
          .build();
  private static final Option RECURSIVE_ALIAS_OPTION =
      Option.builder("r")
          .required(false)
          .hasArg(false)
          .desc("copy files in subdirectories recursively")
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
  private static final Option DELETE_MOUNT_POINT =
      Option.builder("m")
          .longOpt("deleteMountPoint")
          .required(false)
          .hasArg(false)
          .desc("remove mount points in the directory")
          .build();
  private static final Option SYNC_PARENT_NEXT_TIME =
      Option.builder("s")
          .longOpt("syncParentNextTime")
          .required(false)
          .hasArg(true)
          .desc("Marks a directory to either trigger a metadata sync or skip the "
              + "metadata sync on next access.")
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
        .addOption(RECURSIVE_ALIAS_OPTION)
        .addOption(REMOVE_UNCHECKED_OPTION)
        .addOption(REMOVE_ALLUXIO_ONLY)
        .addOption(DELETE_MOUNT_POINT)
        .addOption(SYNC_PARENT_NEXT_TIME);
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    // TODO(calvin): Remove explicit state checking.
    boolean recursive = cl.hasOption(RECURSIVE_OPTION.getOpt())
        || cl.hasOption(RECURSIVE_ALIAS_OPTION.getOpt());
    if (!mFileSystem.exists(path)) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    }
    if (!recursive && mFileSystem.getStatus(path).isFolder()) {
      throw new IOException(path.getPath() + " is a directory, to remove it,"
          + " please use \"rm -R/-r/--recursive <path>\"");
    }
    boolean isAlluxioOnly = cl.hasOption(REMOVE_ALLUXIO_ONLY.getLongOpt());
    boolean isDeleteMountPoint = cl.hasOption(DELETE_MOUNT_POINT.getLongOpt());
    DeletePOptions options =
        DeletePOptions.newBuilder().setRecursive(recursive).setAlluxioOnly(isAlluxioOnly)
            .setDeleteMountPoint(isDeleteMountPoint)
            .setSyncParentNextTime(
                cl.hasOption(SYNC_PARENT_NEXT_TIME.getLongOpt())
                    && Boolean.parseBoolean(cl.getOptionValue(SYNC_PARENT_NEXT_TIME.getLongOpt())))
            .setUnchecked(cl.hasOption(REMOVE_UNCHECKED_OPTION_CHAR)).build();

    mFileSystem.delete(path, options);
    if (!isAlluxioOnly) {
      System.out.println(path + " has been removed");
    } else {
      System.out.println(path + " has been removed only from Alluxio space");
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
    return "rm [-R/-r/--recursive] [-U] [--alluxioOnly] [-m/--deleteMountPoint] <path>";
  }

  @Override
  public String getDescription() {
    return "Removes the specified file. Specify -R/-r/--recursive to remove file"
        + " or directory recursively. Specify -U to remove directories without checking "
        + " UFS contents are in sync. Specify --alluxioOnly to remove data and metadata from"
        + " alluxio space only. Specify -m/--deleteMountPoint to allow removing mount points,"
        + " otherwise the rm will fail if the directory contains mount points.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }
}
