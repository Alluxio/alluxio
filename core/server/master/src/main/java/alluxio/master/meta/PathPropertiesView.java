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

import java.util.Map;

/**
 * Read-only view of path level properties.
 */
public final class PathPropertiesView {
  private final Map<String, Map<String, String>> mProperties;
  private final String mHash;

  /**
   * Constructs a read-only view of path level properties.
   *
   * @param properties map from path to properties
   * @param hash hash of all path level properties
   */
  public PathPropertiesView(Map<String, Map<String, String>> properties, String hash) {
    mProperties = properties;
    mHash = hash;
  }

  /**
   * @return map from path to properties
   */
  public Map<String, Map<String, String>> getProperties() {
    return mProperties;
  }

  /**
   * @return the hash of all path level properties
   */
  public String getHash() {
    return mHash;
  }
}
