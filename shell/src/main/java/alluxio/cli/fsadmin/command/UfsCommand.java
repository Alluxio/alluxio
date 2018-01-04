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

package alluxio.cli.fsadmin.command;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.options.UpdateUfsModeOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.underfs.UnderFileSystem;

/**
 * Update attributes for an existing mount point.
 */
@ThreadSafe
public final class UfsCommand extends AbstractFileSystemAdminCommand {

  private static final Option MODE_OPTION =
      Option.builder()
          .longOpt("mode")
          .required(false)
          .hasArg(true)
          .desc("Set maintenance mode for a ufs path under one or more Alluxio mount points.")
          .build();

  /**
   * @param masterClient the filesystem master client
   */
  public UfsCommand(FileSystemMasterClient masterClient) {
    super(masterClient);
  }

  @Override
  public String getCommandName() {
    return "ufs";
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(MODE_OPTION);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    String ufsPath = args[0];
    if (cl.hasOption(MODE_OPTION.getLongOpt())) {
      UnderFileSystem.UfsMode mode;
      switch(cl.getOptionValue(MODE_OPTION.getLongOpt())) {
        case "noAccess":
          mode = UnderFileSystem.UfsMode.NO_ACCESS;
          break;
        case "readOnly":
          mode = UnderFileSystem.UfsMode.READ_ONLY;
          break;
        case "readWrite":
          mode = UnderFileSystem.UfsMode.READ_WRITE;
          break;
        default:
          System.out.println("Unrecognized mode");
          return -1;
      }
      mMasterClient.updateUfsMode(ufsPath, UpdateUfsModeOptions.defaults().setUfsMode(mode));
      return 0;
    }
    System.out.println("No attribute to update");
    return 0;
  }

  @Override
  public String getUsage() {
    return "ufs [--mode <noAccess/readOnly/readWrite>] <ufsPath>";
  }

  @Override
  public String getDescription() {
    return "Update attributes for a ufs path.";
  }

  @Override
  public void validateArgs(String... args) throws InvalidArgumentException {
    if (args.length != 1) {
      throw new InvalidArgumentException(
          ExceptionMessage.INVALID_ARGS_GENERIC.getMessage(getCommandName()));
    }
  }
}
