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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Unset config for an existing service.
 */
@ThreadSafe
@PublicApi
public final class UnsetConfCommand extends AbstractFsAdminCommand {

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public UnsetConfCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "unsetConf";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    int errorCode = 0;
    Map<PropertyKey, String> properties = new HashMap<>();
    for (String arg : cl.getArgList()) {
      properties.put(PropertyKey.Builder.stringBuilder(arg).buildUnregistered(),
          PropertyKey.UNSET_VALUE);
    }

    if (properties.size() == 0) {
      System.out.println("No config to unset");
      return -1;
    }

    Map<PropertyKey, Boolean> result =
        mMetaConfigClient.updateConfiguration(properties);
    System.out.println("Unset " + result.size() + " configs");
    for (Entry<PropertyKey, Boolean> entry : result.entrySet()) {
      if (!entry.getValue()) {
        System.out.println("Failed to unset " + entry.getKey().getName());
        errorCode = -2;
      }
    }

    return errorCode;
  }

  @Override
  public String getUsage() {
    return "unsetConf key1 [key2 ...]";
  }

  /**
   * @return command's description
   */
  @VisibleForTesting
  public static String description() {
    return "Unset specified configuration keys on Alluxio master.";
  }

  @Override
  public String getDescription() {
    return description();
  }
}
