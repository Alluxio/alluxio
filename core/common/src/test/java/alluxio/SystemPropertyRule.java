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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A rule for setting a system property to a value and then restoring the property to its old value.
 */
public final class SystemPropertyRule extends AbstractResourceRule {
  private final Map<String, String> mProperties;
  private final Map<String, String> mOriginalProperties = new HashMap<>();
  private final Set<String> mOriginalNullProperties = new HashSet<>();

  /**
   * @param keyValuePairs map from system property keys to the values to set them to
   */
  public SystemPropertyRule(Map<String, String> keyValuePairs) {
    mProperties = keyValuePairs;
  }

  /**
   * @param propertyName the name of the property to set
   * @param value the value to set it to, if null un-setting the propertyName
   */
  public SystemPropertyRule(String propertyName, String value) {
    mProperties = new HashMap<>();
    mProperties.put(propertyName, value);
  }

  @Override
  public void before() {
    for (Map.Entry<String, String> entry : mProperties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      String prevValue = System.getProperty(key);
      if (prevValue == null) {
        mOriginalNullProperties.add(key);
      } else {
        mOriginalProperties.put(key, prevValue);
      }
      if (value == null) {
        System.clearProperty(key);
      } else {
        System.setProperty(key, value);
      }
    }
  }

  @Override
  public void after() {
    for (Map.Entry<String, String> entry : mOriginalProperties.entrySet()) {
      System.setProperty(entry.getKey(), entry.getValue());
    }
    for (String key : mOriginalNullProperties) {
      System.clearProperty(key);
    }
  }
}
