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

import alluxio.AlluxioURI;
import alluxio.Client;
import alluxio.conf.PropertyKey;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.wire.ConfigHash;
import alluxio.wire.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Interface for a meta master config client.
 */
public interface MetaMasterConfigClient extends Client {
  /**
   * @param options the options
   * @return the runtime configuration
   */
  Configuration getConfiguration(GetConfigurationPOptions options) throws IOException;

  /**
   * @return hashes of cluster and path level configurations
   */
  ConfigHash getConfigHash() throws IOException;

  /**
   * Sets a property for a path.
   *
   * @param path the path
   * @param key the property key
   * @param value the property value
   */
  default void setPathConfiguration(AlluxioURI path, PropertyKey key, String value)
      throws IOException {
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
  void setPathConfiguration(AlluxioURI path, Map<PropertyKey, String> properties)
      throws IOException;

  /**
   * Removes properties for a path.
   *
   * @param path the path
   * @param keys the property keys
   */
  void removePathConfiguration(AlluxioURI path, Set<PropertyKey> keys) throws IOException;

  /**
   * Removes all properties for a path.
   *
   * @param path the path
   */
  void removePathConfiguration(AlluxioURI path) throws IOException;

  /**
   * Updates properties.
   *
   * @param propertiesMap the properties map to be updated
   * @return the update properties status map
   */
  Map<PropertyKey, Boolean> updateConfiguration(
      Map<PropertyKey, String> propertiesMap) throws IOException;
}
