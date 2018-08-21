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
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.Source;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * <p>
 * Ufs configuration properties, including ufs specific configuration and global configuration.
 *
 * <p>
 * The order of precedence for properties is:
 * <ol>
 * <li>Ufs specific properties</li>
 * <li>Global configuration properties</li>
 * </ol>
 *
 * <p>
 * This class is a wrapper over {@link InstancedConfiguration}. Variable substitution and aliases
 * are supported.
 */
@NotThreadSafe
public final class UnderFileSystemConfiguration {
  private boolean mReadOnly;
  private boolean mShared;
  private InstancedConfiguration mUfsConf;

  /**
   * @return default UFS configuration
   */
  public static UnderFileSystemConfiguration defaults() {
    return new UnderFileSystemConfiguration();
  }

  /**
   * Constructs a new instance of {@link UnderFileSystemConfiguration} with defaults.
   */
  private UnderFileSystemConfiguration() {
    mReadOnly = false;
    mShared = false;
    mUfsConf = new InstancedConfiguration(Configuration.copyProperties());
  }

  /**
   * @param key property key
   * @return true if the key is contained in the given UFS configuration or global configuration
   */
  public boolean containsKey(PropertyKey key) {
    return mUfsConf.isSet(key);
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
    return mUfsConf.get(key);
  }

  /**
   * @return the map of resolved ufs specific configuration
   */
  public Map<String, String> getUfsSpecificConf() {
    Map<String, String> map = new HashMap<>();
    mUfsConf.keySet().forEach(key -> {
      if (mUfsConf.getSource(key) == Source.MOUNT_OPTION) {
        map.put(key.getName(), mUfsConf.get(key));
      }
    });
    return map;
  }

  /**
   * @return all the global and mount specific properties as an immutable map
   */
  public Map<String, String> toMap() {
    return Collections.unmodifiableMap(new HashMap<>(mUfsConf.toMap()));
  }

  /**
   * @return whether only read operations are permitted to the {@link UnderFileSystem}
   */
  public boolean isReadOnly() {
    return mReadOnly;
  }

  /**
   * @return whether the mounted UFS is shared with all Alluxio users
   */
  public boolean isShared() {
    return mShared;
  }

  /**
   * @param readOnly whether only read operations are permitted
   * @return the updated configuration object
   */
  public UnderFileSystemConfiguration setReadOnly(boolean readOnly) {
    mReadOnly = readOnly;
    return this;
  }

  /**
   * @param shared whether the mounted UFS is shared with all Alluxio users
   * @return the updated configuration object
   */
  public UnderFileSystemConfiguration setShared(boolean shared) {
    mShared = shared;
    return this;
  }

  /**
   * @param ufsConf the user-specified UFS configuration as a map
   * @return the updated configuration object
   */
  public UnderFileSystemConfiguration setUserSpecifiedConf(Map<String, String> ufsConf) {
    mUfsConf = new InstancedConfiguration(Configuration.copyProperties());
    mUfsConf.merge(ufsConf, Source.MOUNT_OPTION);
    return this;
  }
}
