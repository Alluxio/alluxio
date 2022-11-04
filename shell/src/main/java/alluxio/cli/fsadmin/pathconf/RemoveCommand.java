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
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.wire.Configuration;

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
          .desc("property keys to be removed, separated by comma")
          .build();
  private static final Option RECURSIVE_OPTION =
      Option.builder("R")
          .longOpt("recursive")
          .desc("remove properties set on paths starting from the specified path recursively")
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
    return new Options().addOption(KEYS_OPTION).addOption(RECURSIVE_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    AlluxioURI path = new AlluxioURI(cl.getArgs()[0]);
    boolean recursive = cl.hasOption(RECURSIVE_OPTION.getOpt());
    if (cl.hasOption(KEYS_OPTION_NAME)) {
      String[] keys = cl.getOptionValue(KEYS_OPTION_NAME).split(",");
      Set<PropertyKey> propertyKeys = new HashSet<>();
      for (String key : keys) {
        propertyKeys.add(PropertyKey.fromString(key));
      }
      handleRemove(path, recursive,
          p -> mMetaConfigClient.removePathConfiguration(p, propertyKeys));
    } else {
      handleRemove(path, recursive, p -> mMetaConfigClient.removePathConfiguration(p));
    }
    return 0;
  }

  private void handleRemove(AlluxioURI path, boolean recursive,
      CheckedConsumer<AlluxioURI> function) throws IOException {
    if (recursive) {
      Configuration conf = mMetaConfigClient.getConfiguration(
          GetConfigurationPOptions.newBuilder().setIgnoreClusterConf(true).build());
      for (String pathWithConf : conf.getPathConf().keySet()) {
        AlluxioURI subPathAlluxioURI = new AlluxioURI(pathWithConf);
        if (isAncestorOf(path, subPathAlluxioURI)) {
          function.consume(subPathAlluxioURI);
        }
      }
    } else {
      function.consume(path);
    }
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

  private boolean isAncestorOf(AlluxioURI prefix, AlluxioURI path) {
    try {
      if (prefix.isAncestorOf(path)) {
        return true;
      }
    } catch (InvalidPathException e) {
      e.printStackTrace();
    }
    return false;
  }

  @FunctionalInterface
  private interface CheckedConsumer<T> {
    void consume(T t) throws IOException;
  }
}
