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
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.path.PathConfiguration;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.wire.Configuration;
import alluxio.wire.Property;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Show path level configurations.
 *
 * <p>
 * It has two modes:
 * 1. without option "--all", only show configurations for the specific path;
 * 2. with option "--all", show all configurations applicable to the path.
 * <p>
 * For example, suppose there are two path level configurations:
 * /a: property1=value1
 * /a/b: property2=value2
 * Then in mode 1, only property2=value2 will be shown for /a/b,
 * but in mode 2, since /a is the prefix of /a/b, both properties will be shown for /a/b.
 */
public final class ShowCommand extends AbstractFsAdminCommand {
  public static final String ALL_OPTION_NAME = "all";

  private static final Option ALL_OPTION =
      Option.builder()
          .longOpt(ALL_OPTION_NAME)
          .required(false)
          .hasArg(false)
          .desc("Whether to show all path level configurations applicable to the path, including"
              + " those set on ancestor paths")
          .build();

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public ShowCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "show";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(ALL_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  private String format(String key, String value) {
    return key + "=" + value;
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    String targetPath = cl.getArgs()[0];
    Configuration configuration = mMetaConfigClient.getConfiguration(
        GetConfigurationPOptions.getDefaultInstance());

    if (cl.hasOption(ALL_OPTION_NAME)) {
      Map<String, AlluxioConfiguration> pathConfMap = new HashMap<>();

      configuration.getPathConf().forEach((path, conf) -> {
        AlluxioProperties properties = new AlluxioProperties();
        conf.forEach(property -> {
          PropertyKey key = PropertyKey.fromString(property.getName());
          properties.set(key, property.getValue());
        });
        pathConfMap.put(path, new InstancedConfiguration(properties));
      });
      PathConfiguration pathConf = PathConfiguration.create(pathConfMap);

      AlluxioURI targetUri = new AlluxioURI(targetPath);
      List<PropertyKey> propertyKeys = new ArrayList<>(pathConf.getPropertyKeys(targetUri));
      propertyKeys.sort(Comparator.comparing(PropertyKey::getName));
      propertyKeys.forEach(key -> {
        pathConf.getConfiguration(targetUri, key).ifPresent(conf -> {
          mPrintStream.println(format(key.getName(), conf.get(key)));
        });
      });
    } else if (configuration.getPathConf().containsKey(targetPath)) {
      List<Property> properties = configuration.getPathConf().get(targetPath);
      properties.sort(Comparator.comparing(Property::getName));
      properties.forEach(property -> {
        mPrintStream.println(format(property.getName(), property.getValue()));
      });
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("show [--%s] <path>%n"
        + "\t--%s: %s",
        ALL_OPTION_NAME,
        ALL_OPTION_NAME, ALL_OPTION.getDescription());
  }

  /**
   * @return command's description
   */
  @VisibleForTesting
  public static String description() {
    return "Shows path level configurations.";
  }

  @Override
  public String getDescription() {
    return description();
  }
}
