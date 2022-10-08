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

import alluxio.AlluxioURI;
import alluxio.cli.CommandUtils;
import alluxio.cli.fsadmin.command.AbstractFsAdminCommand;
import alluxio.cli.fsadmin.command.Context;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.wire.Configuration;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Removes all path level configurations.
 */
public final class RemoveAllCommand extends AbstractFsAdminCommand {

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public RemoveAllCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "removeAll";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 0);
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    Configuration conf = mMetaConfigClient.getConfiguration(
        GetConfigurationPOptions.newBuilder().setIgnoreClusterConf(true).build());
    for (String path : conf.getPathConf().keySet()) {
      mMetaConfigClient.removePathConfiguration(new AlluxioURI(path));
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return getCommandName();
  }

  /**
   * @return command's description
   */
  @VisibleForTesting
  public static String description() {
    return "Removes all properties from all path's path level configurations.";
  }

  @Override
  public String getDescription() {
    return description();
  }
}
