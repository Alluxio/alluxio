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
import alluxio.conf.AlluxioConfiguration;
import alluxio.grpc.UpdateConfigurationPResponse.UpdatePropertyPStatus;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Update config for an existing service.
 */
@ThreadSafe
@PublicApi
public final class UpdateConfCommand extends AbstractFsAdminCommand {

  private static final Option OPTION_OPTION =
      Option.builder()
          .longOpt("option")
          .required(false)
          .hasArg(true)
          .numberOfArgs(2)
          .argName("key=value")
          .valueSeparator('=')
          .desc("property key and value")
          .build();

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public UpdateConfCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(OPTION_OPTION);
  }

  @Override
  public String getCommandName() {
    return "updateConf";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    if (cl.hasOption(OPTION_OPTION.getLongOpt())) {
      Map<String, String> properties = new HashMap<>();
      cl.getOptionProperties(OPTION_OPTION.getLongOpt())
          .forEach((k, v) -> properties.put((String) k, (String) v));
      Map<String, UpdatePropertyPStatus> result
          = mMetaConfigClient.updateConfiguration(properties);
      System.out.println("Updated " + properties.size() + " configs");
      for (Entry<String, UpdatePropertyPStatus> entry : result.entrySet()) {
        if (entry.getValue() != UpdatePropertyPStatus.SUCCESS) {
          System.out.println("Failed to Update " + entry.getKey()
              + " , status is " + entry.getValue());
        }
      }
    } else {
      System.out.println("No config to update");
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "updateConf [--option <key=val>] ";
  }

  /**
   * @return command's description
   */
  @VisibleForTesting
  public static String description() {
    return "Update config for alluxio master.";
  }

  @Override
  public String getDescription() {
    return description();
  }
}
