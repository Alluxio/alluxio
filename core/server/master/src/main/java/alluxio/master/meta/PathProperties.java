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

package alluxio.master.meta;

import alluxio.conf.PropertyKey;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Source of truth for path level properties.
 * TODO(cc): journaling
 * TODO(cc): more operations like batch operation, delete, etc
 */
@ThreadSafe
public final class PathProperties {
  /**
   * Map from path to key value properties.
   */
  private final ConcurrentHashMap<String, ConcurrentHashMap<PropertyKey, String>> mProperties =
      new ConcurrentHashMap<>();

  /**
   * @return an unmodifiable view of the internal properties
   */
  public Map<String, Map<PropertyKey, String>> getProperties() {
    return Collections.unmodifiableMap(mProperties);
  }

  /**
   * Sets a property for a path pattern.
   *
   * @param path the path pattern
   * @param key the property key
   * @param value the property value
   */
  public void setProperty(String path, PropertyKey key, String value) {
    mProperties.computeIfAbsent(path, k -> new ConcurrentHashMap<>()).put(key, value);
  }
}
