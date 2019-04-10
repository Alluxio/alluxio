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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
  private final ConcurrentHashMap<String, AlluxioConfiguration> mConf = new ConcurrentHashMap<>();
  /**
   * Matches path patterns.
   */
  private final PathMatcher mMatcher;

  /**
   * Constructs an empty path level configuration.
   */
  public PrefixPathConfiguration() {
    mMatcher = new PrefixPathMatcher(Collections.emptySet());
  }

  /**
   * Constructs a new path level configuration.
   *
   * A shallow copy of the map will be created internally.
   *
   * @param configurations a map from path patterns to corresponding path level configuration
   */
  public PrefixPathConfiguration(Map<String, AlluxioConfiguration> configurations) {
    configurations.forEach((path, conf) -> mConf.put(path, conf));
    mMatcher = new PrefixPathMatcher(configurations.keySet());
  }

  @Override
  public Optional<AlluxioConfiguration> getConfiguration(AlluxioURI path, PropertyKey key) {
    Optional<List<String>> patterns = mMatcher.match(path);
    if (patterns.isPresent()) {
      for (String pattern : patterns.get()) {
        if (mConf.get(pattern).isSetByUser(key)) {
          return Optional.of(mConf.get(pattern));
        }
      }
    }
    return Optional.empty();
  }

  /**
   * @param path the Alluxio path
   * @return all property keys applicable to path including ancestor paths' configurations
   */
  @Override
  public Set<PropertyKey> getPropertyKeys(AlluxioURI path) {
    Set<PropertyKey> keys = new HashSet<>();
    mMatcher.match(path).ifPresent(patterns -> patterns.forEach(pattern ->
        keys.addAll(mConf.get(pattern).userKeySet())));
    return keys;
  }
}
