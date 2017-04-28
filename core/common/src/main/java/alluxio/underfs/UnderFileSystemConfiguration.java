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

package alluxio.underfs;

import alluxio.Configuration;
import alluxio.PropertyKey;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Gets the value of the given key in the given UFS configuration or the global configuration
 * (in case the key is not found in the UFS configuration), throw {@link RuntimeException} if the
 * key is not found in both configurations..
 */
public class UnderFileSystemConfiguration {
  private final Map<String, String> mUfsConf;

  /**
   * Constructs a new instance of the configuration for a UFS.
   *
   * @param ufsConf the user-specified UFS configuration as a map
   */
  public UnderFileSystemConfiguration(Map<String, String> ufsConf) {
    mUfsConf = ufsConf == null ? Maps.<String, String>newHashMap() : ufsConf;
  }

  /**
   * @param key property key
   * @return true if the key is contained in the given UFS configuration or global configuration
   */
  public boolean containsKey(PropertyKey key) {
    return mUfsConf.containsKey(key.toString()) || Configuration.containsKey(key);
  }

  /**
   * Gets the value of the given key in the given UFS configuration or the global configuration
   * (in case the key is not found in the UFS configuration), throw {@link RuntimeException} if the
   * key is not found in both configurations.
   *
   * @param key property key
   * @return the value associated with the given key
   */
  public String getValue(PropertyKey key) {
    if (mUfsConf.containsKey(key.toString())) {
      return mUfsConf.get(key.toString());
    }
    if (Configuration.containsKey(key)) {
      return Configuration.get(key);
    }
    throw new RuntimeException("key " + key + " not found");
  }


  /**
   * Gets the value of the given key in the given UFS configuration or the global configuration
   * (in case the key is not found in the UFS configuration), throw {@link RuntimeException} if the
   * key is not found in both configurations.
   *
   * @param key property key
   * @param ufsConf configuration for the UFS
   * @return the value associated with the given key
   */
  public static String getValue1(PropertyKey key, Map<String, String> ufsConf) {
    if (ufsConf != null && ufsConf.containsKey(key.toString())) {
      return ufsConf.get(key.toString());
    }
    if (Configuration.containsKey(key)) {
      return Configuration.get(key);
    }
    throw new RuntimeException("key " + key + " not found");
  }

  /**
   * @param key property key
   * @param ufsConf configuration for the UFS
   * @return true if the key is contained in the given UFS configuration or global configuration
   */
  public static boolean containsKey1(PropertyKey key, Map<String, String> ufsConf) {
    return (ufsConf != null && ufsConf.containsKey(key.toString())) || Configuration
        .containsKey(key);
  }
}
