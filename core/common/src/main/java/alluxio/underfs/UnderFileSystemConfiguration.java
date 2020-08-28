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

import alluxio.annotation.PublicApi;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.ConfigurationValueOptions;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.Source;

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
 * This class extends {@link InstancedConfiguration}. Variable substitution and aliases
 * are supported.
 */
@NotThreadSafe
@PublicApi
public final class UnderFileSystemConfiguration extends InstancedConfiguration {
  private boolean mReadOnly;
  private boolean mShared;

  /**
   * @param alluxioConf Alluxio configuration
   * @return ufs configuration from a given alluxio configuration
   */
  public static UnderFileSystemConfiguration defaults(AlluxioConfiguration alluxioConf) {
    return new UnderFileSystemConfiguration(alluxioConf.copyProperties());
  }

  /**
   * Constructs a new instance of {@link UnderFileSystemConfiguration} with the given properties.
   */
  private UnderFileSystemConfiguration(AlluxioProperties props) {
    super(props);
    mReadOnly = false;
    mShared = false;
  }

  /**
   * @return the map of resolved mount specific configuration
   */
  public Map<String, String> getMountSpecificConf() {
    Map<String, String> map = new HashMap<>();
    keySet().forEach(key -> {
      if (getSource(key) == Source.MOUNT_OPTION) {
        map.put(key.getName(), get(key));
      }
    });
    return map;
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
   * Creates a new instance from the current configuration and adds in new properties.
   * @param mountConf the mount specific configuration map
   * @return the updated configuration object
   */
  public UnderFileSystemConfiguration createMountSpecificConf(Map<String, String> mountConf) {
    UnderFileSystemConfiguration ufsConf = new UnderFileSystemConfiguration(mProperties.copy());
    ufsConf.mProperties.merge(mountConf, Source.MOUNT_OPTION);
    ufsConf.mReadOnly = mReadOnly;
    ufsConf.mShared = mShared;
    return ufsConf;
  }

  /**
   * @param options options for formatting the configuration values
   * @return a map from all user configuration property names to their values; values may
   * potentially be null
   */
  public Map<String, String> toUserPropertyMap(ConfigurationValueOptions options) {
    Map<String, String> map = new HashMap<>();
    // Cannot use Collectors.toMap because we support null keys.
    userKeySet().forEach(key -> map.put(key.getName(), getOrDefault(key, null, options)));
    return map;
  }
}
