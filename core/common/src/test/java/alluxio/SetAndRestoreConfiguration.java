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
 * An AutoCloseable which temporarily modifies configuration when it is constructed and
 * restores the property when it is closed.
 */
public final class SetAndRestoreConfiguration implements AutoCloseable {
  private final Map<PropertyKey, String> mOriginalValues = new HashMap<>();
  private final Set<PropertyKey> mOriginalNullKeys = new HashSet<>();

  /**
   * @param key the key of the configuration property to set
   * @param value the value to set it to
   */
  public SetAndRestoreConfiguration(PropertyKey key, String value) {
    this(ImmutableMap.of(key, value));
  }

  /**
   * @param keyValuePairs the key value pairs of the configuration property to set
   */
  public SetAndRestoreConfiguration(Map<PropertyKey, String> keyValuePairs) {
    for (Map.Entry<PropertyKey, String> entry : keyValuePairs.entrySet()) {
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
  public void close() throws Exception {
    for (Map.Entry<PropertyKey, String> entry : mOriginalValues.entrySet()) {
      Configuration.set(entry.getKey(), entry.getValue());
    }
    for (PropertyKey key : mOriginalNullKeys) {
      Configuration.unset(key);
    }
  }
}
