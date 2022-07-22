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

import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.CleanOrphanBlocksPOptions;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

/**
 * Command for cleaning up orphan block.
 */
@PublicApi
public class CleanCommand extends AbstractFsAdminCommand {

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public CleanCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "clean";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    CleanOrphanBlocksPOptions options = CleanOrphanBlocksPOptions.newBuilder().build();
    mFsClient.cleanOrphanBlocks(options);
    System.out.println("Clean up orphan block completed.");
    return 0;
  }

  @Override
  public String getUsage() {
    return usage();
  }

  private String usage() {
    return "clean";
  }

  @Override
  public String getDescription() {
    return "clean up redundant orphan blocks in the alluxio";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 0);
  }
}
