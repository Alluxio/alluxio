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
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.UnmountPOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Unmounts an Alluxio path.
 */
@ThreadSafe
@PublicApi
public final class UnmountCommand extends AbstractFileSystemCommand {
  private static final Option FORCE_OPTION =
      Option.builder("f")
          .required(false)
          .hasArg(false)
          .desc("force to unmount")
          .build();
  private static final Option UFS_PATH_OPTION =
      Option.builder("u")
          .required(false)
          .hasArg(true)
          .desc("force to unmount")
          .build();

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public UnmountCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "unmount";
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(FORCE_OPTION)
        .addOption(UFS_PATH_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
    if (cl.hasOption(FORCE_OPTION.getOpt()) && !cl.hasOption(UFS_PATH_OPTION.getOpt())) {
      throw new InvalidArgumentException("-" + UFS_PATH_OPTION.getOpt() + " must provided while -"
          + FORCE_OPTION.getOpt() + " exist");
    }
  }

  @Override
  protected void runPlainPath(AlluxioURI inputPath, CommandLine cl)
      throws AlluxioException, IOException {
    boolean force = cl.hasOption(FORCE_OPTION.getOpt());
    AlluxioURI ufsPath = null;
    if (force) {
      ufsPath = new AlluxioURI(cl.getOptionValue(UFS_PATH_OPTION.getOpt()));
    }
    mFileSystem.unmount(inputPath, ufsPath, UnmountPOptions.newBuilder().setForced(force).build());
    System.out.println("Unmounted " + inputPath);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI inputPath = new AlluxioURI(args[0]);
    runWildCardCmd(inputPath, cl);
    return 0;
  }

  @Override
  public String getUsage() {
    return "unmount [-f <-u ufsPath>] <alluxioPath>";
  }

  @Override
  public String getDescription() {
    return "Unmounts an Alluxio path.";
  }
}
