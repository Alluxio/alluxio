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
    int errorCode = 0;
    Map<String, String> properties = new HashMap<>();
    cl.getArgList().forEach(arg -> {
      if (arg.contains("=")) {
        String[] kv = arg.split("=");
        if (arg.length() == 2) {
          properties.put(kv[0], kv[1]);
        }
      }
    });

    if (properties.size() > 0) {
      Map<String, Boolean> result
          = mMetaConfigClient.updateConfiguration(properties);
      System.out.println("Updated " + result.size() + " configs");
      for (Entry<String, Boolean> entry : result.entrySet()) {
        if (!entry.getValue()) {
          System.out.println("Failed to Update " + entry.getKey());
          errorCode = -2;
        }
      }
    } else {
      System.out.println("No config to update");
      errorCode = -1;
    }

    return errorCode;
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
