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

package alluxio.network;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.PropertyKey.Template;
import alluxio.util.ShellUtils;
import alluxio.wire.TieredIdentity;
import alluxio.wire.TieredIdentity.LocalityTier;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Class for getting tiered identity.
 */
public final class TieredIdentityFactory {
  private static TieredIdentity sInstance = null;

  /**
   * @return the singleton tiered identity instance for this JVM
   */
  public static TieredIdentity getInstance() {
    synchronized (sInstance) {
      if (sInstance == null) {
        sInstance = create();
      }
      return sInstance;
    }
  }

  /**
   * Creates a tiered identity based on configuration.
   *
   * @return the created tiered identity
   */
  public static TieredIdentity create() {
    TieredIdentity scriptIdentity = null;
    // If a script is configured, run the script to get all locality tiers.
    if (Configuration.containsKey(PropertyKey.LOCALITY_SCRIPT)) {
      scriptIdentity = fromScript();
    }

    List<LocalityTier> tiers = new ArrayList<>();
    List<String> orderedTierNames = Configuration.getList(PropertyKey.LOCALITY_ORDER, ",");
    for (int i = 0; i < orderedTierNames.size(); i++) {
      String tierName = orderedTierNames.get(i);
      String value = null;
      if (scriptIdentity != null) {
        LocalityTier scriptTier = scriptIdentity.getTiers().get(i);
        Preconditions.checkState(scriptTier.getTierName().equals(tierName));
        value = scriptTier.getValue();
      }
      // Explicit configuration overrides script output.
      if (Configuration.containsKey(Template.LOCALITY_TIER.format(tierName))) {
        value = Configuration.get(Template.LOCALITY_TIER.format(tierName));
      }
      tiers.add(new LocalityTier(tierName, value));
    }
    return new TieredIdentity(tiers);
  }

  /**
   * @return a tiered identity created from running the user-provided script
   */
  public static TieredIdentity fromScript() {
    String script = Configuration.get(PropertyKey.LOCALITY_SCRIPT);
    String identityString;
    try {
      identityString = ShellUtils.execCommand(script);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to run script %s: %s", script, e.toString(), e));
    }
    TieredIdentity identity = parseIdentityString(identityString);
    if (identity == null) {
      throw new RuntimeException(String.format(
          "Failed to parse the output of running %s. "
              + "The value should be a comma-separated list of key=value pairs, but was %s",
          script, identityString));
    }
    return identity;
  }

  /**
   * @param identityString tiered identity string to parse
   * @return the parsed tiered identity, or null if the string could not be parsed
   */
  @Nullable
  public static TieredIdentity parseIdentityString(String identityString) {
    Map<String, String> tiers = new HashMap<>();
    for (String tier : identityString.split(",")) {
      String[] parts = tier.split("=");
      if (parts.length != 2) {
        return null;
      }
      tiers.put(parts[0].trim(), parts[1].trim());
    }
    List<LocalityTier> tieredIdentity = new ArrayList<>();
    for (String localityTier : Configuration.getList(PropertyKey.LOCALITY_ORDER, ",")) {
      String value = tiers.containsKey(localityTier) ? tiers.get(localityTier) : null;
      tieredIdentity.add(new LocalityTier(localityTier, value));
    }
    return new TieredIdentity(tieredIdentity);
  }
}
