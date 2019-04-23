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

import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.PropertyKey.Template;
import alluxio.util.ShellUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.TieredIdentity;
import alluxio.wire.TieredIdentity.LocalityTier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * Class for getting tiered identity.
 */
public final class TieredIdentityFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TieredIdentityFactory.class);

  // Synchronize on this lock to modify sInstance.
  private static final Object LOCK = new Object();
  @GuardedBy("LOCK")
  private static volatile TieredIdentity sInstance = null;

  /**
   * @param conf Alluxio configuration
   * @return the singleton tiered identity instance for this JVM
   */
  public static TieredIdentity localIdentity(AlluxioConfiguration conf) {
    if (sInstance == null) {
      synchronized (LOCK) {
        if (sInstance == null) {
          sInstance = create(conf);
          LOG.info("Initialized tiered identity {}", sInstance);
        }
      }
    }
    return sInstance;
  }

  /**
   * Creates a tiered identity based on configuration.
   *
   * @return the created tiered identity
   */
  @VisibleForTesting
  static TieredIdentity create(AlluxioConfiguration conf) {
    TieredIdentity scriptIdentity = fromScript(conf);

    List<LocalityTier> tiers = new ArrayList<>();
    List<String> orderedTierNames = conf.getList(PropertyKey.LOCALITY_ORDER, ",");
    for (int i = 0; i < orderedTierNames.size(); i++) {
      String tierName = orderedTierNames.get(i);
      String value = null;
      if (scriptIdentity != null) {
        LocalityTier scriptTier = scriptIdentity.getTier(i);
        Preconditions.checkState(scriptTier.getTierName().equals(tierName));
        value = scriptTier.getValue();
      }
      // Explicit configuration overrides script output.
      if (conf.isSet(Template.LOCALITY_TIER.format(tierName))) {
        value = conf.get(Template.LOCALITY_TIER.format(tierName));
      }
      tiers.add(new LocalityTier(tierName, value));
    }
    // If the user doesn't specify the value of the "node" tier, we fill in a sensible default.
    if (tiers.size() > 0 && tiers.get(0).getTierName().equals(Constants.LOCALITY_NODE)
        && tiers.get(0).getValue() == null) {
      String name = NetworkAddressUtils.getLocalNodeName(conf);
      tiers.set(0, new LocalityTier(Constants.LOCALITY_NODE, name));
    }
    return new TieredIdentity(tiers);
  }

  /**
   * @param conf Alluxio configuration
   * @return a tiered identity created from running the user-provided script
   */
  @Nullable
  private static TieredIdentity fromScript(AlluxioConfiguration conf) {
    String scriptName = conf.get(PropertyKey.LOCALITY_SCRIPT);
    Path script = Paths.get(scriptName);
    if (!Files.exists(script)) {
      URL resource = TieredIdentityFactory.class.getClassLoader().getResource(scriptName);
      if (resource != null) {
        script = Paths.get(resource.getPath());
      } else {
        return null;
      }
    }
    LOG.debug("Found tiered identity script at {}", script);
    String identityString;
    try {
      identityString = ShellUtils.execCommand(script.toString());
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to run script %s: %s", script, e.toString()), e);
    }
    try {
      return fromString(identityString, conf);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to parse output of running %s: %s", script, e.getMessage()), e);
    }
  }

  /**
   * @param identityString tiered identity string to parse
   * @param conf Alluxio configuration
   * @return the parsed tiered identity
   */
  public static TieredIdentity fromString(String identityString, AlluxioConfiguration conf)
      throws IOException {
    Set<String> allTiers = Sets.newHashSet(conf.getList(PropertyKey.LOCALITY_ORDER, ","));
    Map<String, String> tiers = new HashMap<>();
    for (String tier : identityString.split(",")) {
      String[] parts = tier.split("=");
      if (parts.length != 2) {
        throw new IOException(String
            .format("Failed to parse tiered identity. The value should be a comma-separated list "
                + "of key=value pairs, but was %s", identityString));
      }
      String key = parts[0].trim();
      if (tiers.containsKey(key)) {
        throw new IOException(String.format(
            "Encountered repeated tier definition for %s when parsing tiered identity from string "
                + "%s",
            key, identityString));
      }
      if (!allTiers.contains(key)) {
        throw new IOException(String.format("Unrecognized tier: %s. The tiers defined by %s are %s",
            key, PropertyKey.LOCALITY_ORDER.toString(), allTiers));
      }
      tiers.put(key, parts[1].trim());
    }
    List<LocalityTier> tieredIdentity = new ArrayList<>();
    for (String localityTier : conf.getList(PropertyKey.LOCALITY_ORDER, ",")) {
      String value = tiers.getOrDefault(localityTier, null);
      tieredIdentity.add(new LocalityTier(localityTier, value));
    }
    return new TieredIdentity(tieredIdentity);
  }
}
