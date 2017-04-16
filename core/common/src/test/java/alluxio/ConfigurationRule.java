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

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A rule for modifying Alluxio configuration during a test suite.
 */
public final class ConfigurationRule extends AbstractResourceRule {
  private final Map<PropertyKey, String> mKeyValuePairs;
  private final Map<PropertyKey, String> mOriginalValues = new HashMap<>();
  private final Set<PropertyKey> mOriginalNullKeys = new HashSet<>();

  /**
   * @param keyValuePairs map from configuration keys to the values to set them to
   */
  public ConfigurationRule(Map<PropertyKey, String> keyValuePairs) {
    mKeyValuePairs = keyValuePairs;
  }

  /**
   * @param key the key of the configuration property to set
   * @param value the value to set it to
   */
  public ConfigurationRule(PropertyKey key, String value) {
    this(ImmutableMap.of(key, value));
  }

  @Override
  public void before() {
    for (Map.Entry<PropertyKey, String> entry : mKeyValuePairs.entrySet()) {
      PropertyKey key = entry.getKey();
      String value = entry.getValue();
      if (Configuration.containsKey(key)) {
        mOriginalValues.put(key, Configuration.get(key));
      } else {
        mOriginalNullKeys.add(key);
      }
      Configuration.set(key, value);
    }
  }

  @Override
  public void after() {
    for (Map.Entry<PropertyKey, String> entry : mOriginalValues.entrySet()) {
      Configuration.set(entry.getKey(), entry.getValue());
    }
    for (PropertyKey key : mOriginalNullKeys) {
      Configuration.unset(key);
    }
  }
}
