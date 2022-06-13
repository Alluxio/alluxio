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
  private final Map<PropertyKey, Object> mKeyValuePairs;
  private final Map<PropertyKey, Object> mStashedProperties = new HashMap<>();
  private final InstancedConfiguration mConfiguration;

  /**
   * @param keyValuePairs map from configuration keys to the values to set them to
   * @param conf base configuration
   */
  public ConfigurationRule(Map<PropertyKey, Object> keyValuePairs, InstancedConfiguration conf) {
    mKeyValuePairs = keyValuePairs;
    mConfiguration = conf;
  }

  /**
   * @param key the key of the configuration property to set
   * @param value the value to set it to, can be null to unset this key
   * @param conf base configuration
   */
  public ConfigurationRule(final PropertyKey key, final Object value, InstancedConfiguration conf) {
    // ImmutableMap does not support nullable value, create a map literals
    this(new HashMap<PropertyKey, Object>() {
      {
        put(key, value);
      }
    }, conf);
  }

  /**
   * Set a specific PropertyKey of the global configuration,
   * change is only visible in the scope of the calling method.
   *
   * @param key the key of the configuration property to set
   * @param value the value to set it to, can be null to unset
   */
  public ConfigurationRule set(final PropertyKey key, final Object value) {
    if (!mStashedProperties.containsKey(key)) {
      if (mConfiguration.isSet(key)) {
        mStashedProperties.put(key, mConfiguration.get(key));
      } else {
        mStashedProperties.put(key, null);
      }
    }

    if (value != null) {
      mConfiguration.set(key, value);
    } else {
      mConfiguration.unset(key);
    }

    return this;
  }

  @Override
  public void before() {
    for (Map.Entry<PropertyKey, Object> entry : mKeyValuePairs.entrySet()) {
      PropertyKey key = entry.getKey();
      Object value = entry.getValue();
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
    for (Map.Entry<PropertyKey, Object> entry : mStashedProperties.entrySet()) {
      Object value = entry.getValue();
      if (value != null) {
        mConfiguration.set(entry.getKey(), value);
      } else {
        mConfiguration.unset(entry.getKey());
      }
    }
  }
}
