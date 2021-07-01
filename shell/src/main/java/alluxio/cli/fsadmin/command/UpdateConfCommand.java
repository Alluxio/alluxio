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
import alluxio.conf.PropertyKey;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;

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

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public UpdateConfCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "updateConf";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    int errorCode = 0;
    Map<PropertyKey, String> properties = new HashMap<>();
    for (String arg : cl.getArgList()) {
      if (arg.contains("=")) {
        String[] kv = arg.split("=");
        if (kv.length != 2) {
          System.err.println(
              "Failed to parse %s, expecting argument in the format of \"key=val\", arg)");
          return -3;
        }
        properties.put(PropertyKey.getOrBuildCustom(kv[0]), kv[1]);
      }
    }

    if (properties.size() == 0) {
      System.out.println("No config to update");
      return -1;
    }

    Map<PropertyKey, Boolean> result =
        mMetaConfigClient.updateConfiguration(properties);
    System.out.println("Updated " + result.size() + " configs");
    for (Entry<PropertyKey, Boolean> entry : result.entrySet()) {
      if (!entry.getValue()) {
        System.out.println("Failed to Update " + entry.getKey().getName());
        errorCode = -2;
      }
    }

    return errorCode;
  }

  @Override
  public String getUsage() {
    return "updateConf key1=val1 [key2=val2 ...]";
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
