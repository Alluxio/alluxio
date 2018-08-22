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
import alluxio.annotation.PublicApi;
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
 * This class extends {@link InstancedConfiguration}. Variable substitution and aliases
 * are supported.
 */
@NotThreadSafe
@PublicApi
public final class UnderFileSystemConfiguration extends InstancedConfiguration {
  private boolean mReadOnly;
  private boolean mShared;

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
    super(Configuration.copyProperties());
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
   * @param mountConf the mount specific configuration map
   * @return the updated configuration object
   */
  public UnderFileSystemConfiguration setMountSpecificConf(Map<String, String> mountConf) {
    mProperties = Configuration.copyProperties();
    merge(mountConf, Source.MOUNT_OPTION);
    return this;
  }
}
