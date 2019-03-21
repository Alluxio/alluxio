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
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Configurations for path prefixes.
 *
 * Path patterns are path prefixes, the longest matching prefix is the best match.
 * A path is equal to the matched prefix or is under the subtree of the prefix.
 */
@ThreadSafe
public final class PrefixPathConfiguration implements PathConfiguration {
  /**
   * Map from path prefixes to corresponding configurations.
   */
  private ConcurrentHashMap<String, AlluxioConfiguration> mConf = new ConcurrentHashMap<>();
  /**
   * Matches path patterns.
   */
  private PathMatcher mMatcher;

  /**
   * Constructs an empty path level configuration.
   */
  public PrefixPathConfiguration() {
    mMatcher = new PrefixPathMatcher(Collections.emptySet());
  }

  /**
   * Constructs a new path level configuration based on the path level properties.
   *
   * @param pathProperties a map from path patterns to corresponding properties
   */
  public PrefixPathConfiguration(Map<String, AlluxioProperties> pathProperties) {
    pathProperties.forEach(
        (path, properties) -> mConf.put(path, new InstancedConfiguration(properties)));
    mMatcher = new PrefixPathMatcher(pathProperties.keySet());
  }

  @Override
  public Optional<AlluxioConfiguration> getConfiguration(AlluxioURI path, PropertyKey key) {
    Optional<List<String>> patterns = mMatcher.match(path);
    if (patterns.isPresent()) {
      for (String pattern : patterns.get()) {
        if (mConf.get(pattern).isSet(key)) {
          return Optional.of(mConf.get(pattern));
        }
      }
    }
    return Optional.empty();
  }
}
