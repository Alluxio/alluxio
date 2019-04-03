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

package alluxio.cli.fsadmin.pathconf;

import alluxio.cli.CommandUtils;
import alluxio.cli.fsadmin.command.AbstractFsAdminCommand;
import alluxio.cli.fsadmin.command.Context;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.wire.Configuration;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

/**
 * Lists paths that have path level configuration.
 */
public final class ListCommand extends AbstractFsAdminCommand {
  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public ListCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "list";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 0);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    Configuration conf = mMetaConfigClient.getConfiguration();
    for (String path : conf.getPathConf().keySet()) {
      mPrintStream.println(path);
    }
    mPrintStream.close();
    return 0;
  }

  @Override
  public String getUsage() {
    return "list";
  }

  @Override
  public String getDescription() {
    return "List paths that have path level configuration.";
  }
}
