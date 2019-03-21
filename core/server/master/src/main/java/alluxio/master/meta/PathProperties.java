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

import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Source of truth for path level properties.
 * TODO(cc): journaling
 * TODO(cc): improve concurrency
 * TODO(cc): more operations like batch operation, delete, etc
 */
@ThreadSafe
public final class PathProperties {
  /**
   * Map from path to path level properties.
   */
  private final ConcurrentHashMap<String, AlluxioProperties> mProperties =
      new ConcurrentHashMap<>();

  /**
   * @return a deep copy of the internal map from path to properties
   */
  public synchronized Map<String, AlluxioProperties> getProperties() {
    Map<String, AlluxioProperties> properties = new HashMap<>();
    mProperties.forEach((path, props) -> properties.put(path, props.copy()));
    return properties;
  }

  /**
   * Sets a property for a path pattern.
   *
   * @param path the path pattern
   * @param key the property key
   * @param value the property value
   */
  public synchronized void setProperty(String path, PropertyKey key, String value) {
    mProperties.computeIfAbsent(path, k -> new AlluxioProperties())
        .put(key, value, Source.PATH_DEFAULT);
  }
}
