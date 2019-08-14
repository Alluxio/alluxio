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
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.Scope;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Adds path level configurations.
 */
public final class AddCommand extends AbstractFsAdminCommand {
  public static final String PROPERTY_OPTION_NAME = "property";

  private static final Option PROPERTY_OPTION =
      Option.builder()
          .longOpt(PROPERTY_OPTION_NAME)
          .required(false)
          .hasArg(true)
          .numberOfArgs(2)
          .argName("key=value")
          .valueSeparator('=')
          .desc("property associated with the path")
          .build();

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public AddCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "add";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(PROPERTY_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    AlluxioURI path = new AlluxioURI(cl.getArgs()[0]);
    Map<PropertyKey, String> propertyMap = new HashMap<>();
    if (cl.hasOption(PROPERTY_OPTION_NAME)) {
      Map<String, String> properties = Maps.fromProperties(
          cl.getOptionProperties(PROPERTY_OPTION_NAME));
      for (Map.Entry<String, String> property : properties.entrySet()) {
        PropertyKey key = PropertyKey.fromString(property.getKey());
        if (!GrpcUtils.contains(key.getScope(), Scope.CLIENT)) {
          throw new InvalidArgumentException(nonClientScopePropertyException(key));
        }
        propertyMap.put(key, property.getValue());
      }
    }
    mMetaConfigClient.setPathConfiguration(path, propertyMap);
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s [--%s <key=value>] [--%s <key=value>] <path>%n"
        + "\t--%s: %s",
        getCommandName(), PROPERTY_OPTION_NAME, PROPERTY_OPTION_NAME,
        PROPERTY_OPTION_NAME, PROPERTY_OPTION.getDescription());
  }

  /**
   * @return command's description
   */
  @VisibleForTesting
  public static String description() {
    return "Adds properties to the path level configurations. "
        + "Only client scope properties can be added.";
  }

  /**
   * @param key the property key
   * @return the exception message for a non-client scope property key
   */
  @VisibleForTesting
  public static String nonClientScopePropertyException(PropertyKey key) {
    return "Only properties with scope containing client scope can be set for path level "
        + "configuration, but property key " + key + " has scope " + key.getScope().toString();
  }

  @Override
  public String getDescription() {
    return description();
  }
}
