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

package alluxio;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import java.util.HashMap;
import java.util.Map;

/**
 * A rule for modifying Alluxio configuration during a test suite.
 */
public final class ConfigurationRule extends AbstractResourceRule {
  private final Map<PropertyKey, String> mKeyValuePairs;
  private final Map<PropertyKey, String> mStashedProperties = new HashMap<>();
  private final InstancedConfiguration mConfiguration;

  /**
   * @param keyValuePairs map from configuration keys to the values to set them to
   * @param conf base configuration
   */
  public ConfigurationRule(Map<PropertyKey, String> keyValuePairs, InstancedConfiguration conf) {
    mKeyValuePairs = keyValuePairs;
    mConfiguration = conf;
  }

  /**
   * @param key the key of the configuration property to set
   * @param value the value to set it to, can be null to unset this key
   * @param conf base configuration
   */
  public ConfigurationRule(final PropertyKey key, final String value, InstancedConfiguration conf) {
    // ImmutableMap does not support nullable value, create a map literals
    this(new HashMap<PropertyKey, String>() {
      {
        put(key, value);
      }
    }, conf);
  }

  @Override
  public void before() {
    for (Map.Entry<PropertyKey, String> entry : mKeyValuePairs.entrySet()) {
      PropertyKey key = entry.getKey();
      String value = entry.getValue();
      if (mConfiguration.isSet(key)) {
        mStashedProperties.put(key, mConfiguration.get(key));
      } else {
        mStashedProperties.put(key, null);
      }
      if (value != null) {
        mConfiguration.set(key, value);
      } else {
        mConfiguration.unset(key);
      }
    }
  }

  @Override
  public void after() {
    for (Map.Entry<PropertyKey, String> entry : mStashedProperties.entrySet()) {
      String value = entry.getValue();
      if (value != null) {
        mConfiguration.set(entry.getKey(), value);
      } else {
        mConfiguration.unset(entry.getKey());
      }
    }
  }
}
