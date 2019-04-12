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

package alluxio.client.meta;

import alluxio.conf.PropertyKey;
import alluxio.wire.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Interface for a meta master config client.
 */
public interface MetaMasterConfigClient extends Closeable {
  /**
   * @return the runtime configuration
   */
  Configuration getConfiguration() throws IOException;

  /**
   * Sets a property for a path.
   *
   * @param path the path
   * @param key the proeprty key
   * @param value the property value
   */
  default void setPathConfiguration(String path, PropertyKey key, String value) throws IOException {
    Map<PropertyKey, String> properties = new HashMap<>();
    properties.put(key, value);
    setPathConfiguration(path, properties);
  }

  /**
   * Sets properties for a path.
   *
   * @param path the path
   * @param properties the properties
   */
  void setPathConfiguration(String path, Map<PropertyKey, String> properties) throws IOException;

  /**
   * Removes properties for a path.
   *
   * @param path the path
   * @param keys the property keys
   */
  void removePathConfiguration(String path, Set<PropertyKey> keys) throws IOException;

  /**
   * Removes all properties for a path.
   *
   * @param path the path
   */
  void removePathConfiguration(String path) throws IOException;
}
