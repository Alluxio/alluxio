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

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.UfsPMode;
import alluxio.grpc.UpdateUfsModePOptions;
import alluxio.util.io.PathUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Update attributes for an existing mount point.
 */
@ThreadSafe
@PublicApi
public final class UfsCommand extends AbstractFsAdminCommand {
  private static final Option MODE_OPTION =
      Option.builder()
          .longOpt("mode")
          .required(false)
          .hasArg(true)
          .desc("Set maintenance mode for a ufs path under one or more Alluxio mount points.")
          .build();

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public UfsCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "ufs";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(MODE_OPTION);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    String ufsPath = args[0];
    AlluxioURI ufsUri = new AlluxioURI(ufsPath);
    if (!PathUtils.normalizePath(ufsUri.getPath(), AlluxioURI.SEPARATOR)
        .equals(AlluxioURI.SEPARATOR)) {
      System.out.println("The ufs path should have only scheme and authority but no path.");
      return -1;
    }
    if (cl.hasOption(MODE_OPTION.getLongOpt())) {
      UfsPMode mode;
      switch (cl.getOptionValue(MODE_OPTION.getLongOpt())) {
        case "noAccess":
          mode = UfsPMode.NO_ACCESS;
          break;
        case "readOnly":
          mode = UfsPMode.READ_ONLY;
          break;
        case "readWrite":
          mode = UfsPMode.READ_WRITE;
          break;
        default:
          System.out.println("Unrecognized mode");
          return -1;
      }
      UpdateUfsModePOptions options = UpdateUfsModePOptions.newBuilder().setUfsMode(mode).build();
      mFsClient.updateUfsMode(ufsUri, options);
      System.out.println("Ufs mode updated");
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
}
