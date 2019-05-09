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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Removes path level configurations.
 */
public final class RemoveCommand extends AbstractFsAdminCommand {
  public static final String KEYS_OPTION_NAME = "keys";

  private static final Option KEYS_OPTION =
      Option.builder()
          .longOpt(KEYS_OPTION_NAME)
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .desc("properties keys to be removed from this path's configurations, separated by comma")
          .build();

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public RemoveCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "remove";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(KEYS_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    AlluxioURI path = new AlluxioURI(cl.getArgs()[0]);
    if (cl.hasOption(KEYS_OPTION_NAME)) {
      String[] keys = cl.getOptionValue(KEYS_OPTION_NAME).split(",");
      Set<PropertyKey> propertyKeys = new HashSet<>();
      for (String key : keys) {
        propertyKeys.add(PropertyKey.fromString(key));
      }
      mMetaConfigClient.removePathConfiguration(path, propertyKeys);
    } else {
      mMetaConfigClient.removePathConfiguration(path);
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s [--%s <key1,key2,key3>] <path>%n"
        + "\t--%s: %s",
        getCommandName(), KEYS_OPTION_NAME,
        KEYS_OPTION_NAME, KEYS_OPTION.getDescription());
  }

  /**
   * @return command's description
   */
  @VisibleForTesting
  public static String description() {
    return "Removes all or specific properties from path's path level configurations.";
  }

  @Override
  public String getDescription() {
    return description();
  }
}
