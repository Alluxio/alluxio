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

package alluxio.conf.path;

import alluxio.AlluxioURI;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Path level configuration.
 *
 * An {@link AlluxioConfiguration} is set for each path pattern. This class matches an Alluxio
 * path against the patterns, then from the configurations of the matched patterns, it chooses the
 * configuration that contains the specified property key and belongs to the best matched pattern.
 *
 * Different implementations have different definitions of the best match.
 */
public interface PathConfiguration {
  /**
   * @param path the Alluxio path
   * @param key the property key
   * @return the chosen configuration matching the path and containing the key
   */
  Optional<AlluxioConfiguration> getConfiguration(AlluxioURI path, PropertyKey key);

  /**
   * @param path the Alluxio path
   * @return all property keys in the path level configuration that is applicable to path
   */
  Set<PropertyKey> getPropertyKeys(AlluxioURI path);

  /**
   * Factory method to create an implementation of {@link PathConfiguration}.
   *
   * @param pathConf the map from paths to path level configurations
   * @return the implementation of {@link PathConfiguration}
   */
  static PathConfiguration create(Map<String, AlluxioConfiguration> pathConf) {
    return new PrefixPathConfiguration(pathConf);
  }
}
