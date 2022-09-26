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
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
public final class UnderFileSystemConfiguration implements AlluxioConfiguration {
  private final boolean mReadOnly;
  private final AlluxioConfiguration mAlluxioConf;
  private final boolean mCrossCluster;
  private static final UnderFileSystemConfiguration EMPTY_CONFIG =
      new UnderFileSystemConfiguration(new InstancedConfiguration(new AlluxioProperties()),
          false, false);

  /**
   * @param alluxioConf Alluxio configuration
   * @return ufs configuration from a given alluxio configuration
   */
  public static UnderFileSystemConfiguration defaults(AlluxioConfiguration alluxioConf) {
    return new UnderFileSystemConfiguration(alluxioConf, false, false);
  }

  /**
   * @return ufs configuration with empty config
   */
  public static UnderFileSystemConfiguration emptyConfig() {
    return EMPTY_CONFIG;
  }

  /**
   * Constructs a new instance of {@link UnderFileSystemConfiguration} with the given properties.
   * @param alluxioConf Alluxio configuration
   * @param readOnly whether only read operations are permitted
   * @param crossCluster if cross cluster sync is enabled
   */
  public UnderFileSystemConfiguration(
      AlluxioConfiguration alluxioConf, boolean readOnly, boolean crossCluster) {
    mAlluxioConf = alluxioConf;
    mReadOnly = readOnly;
    mCrossCluster = crossCluster;
  }

  /**
   * @return the map of resolved mount specific configuration
   */
  public Map<String, Object> getMountSpecificConf() {
    Map<String, Object> map = new HashMap<>();
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
   * @return true if cross cluster sync is enabled
   */
  public boolean isCrossCluster() {
    return mCrossCluster;
  }

  /**
   * Creates a new instance from the current configuration and adds in new properties.
   * @param mountConf the mount specific configuration map
   * @return the updated configuration object
   */
  public UnderFileSystemConfiguration createMountSpecificConf(
      Map<String, ? extends Object> mountConf) {
    AlluxioProperties properties = copyProperties();
    properties.merge(mountConf, Source.MOUNT_OPTION);
    return new UnderFileSystemConfiguration(
        new InstancedConfiguration(properties), mReadOnly, mCrossCluster);
  }

  /**
   * @param options options for formatting the configuration values
   * @return a map from all user configuration property names to their values; values may
   * potentially be null
   */
  public Map<String, String> toUserPropertyMap(ConfigurationValueOptions options) {
    Map<String, String> map = new HashMap<>();
    // Cannot use Collectors.toMap because we support null keys.
    userKeySet().forEach(key -> {
      Object value = getOrDefault(key, null, options);
      map.put(key.getName(), value == null ? null : String.valueOf(value));
    });
    return map;
  }

  @Override
  public Object get(PropertyKey key)
  {
    return mAlluxioConf.get(key);
  }

  @Override
  public Object get(PropertyKey key, ConfigurationValueOptions options)
  {
    return mAlluxioConf.get(key, options);
  }

  @Override
  public boolean isSet(PropertyKey key)
  {
    return mAlluxioConf.isSet(key);
  }

  @Override
  public boolean isSetByUser(PropertyKey key)
  {
    return mAlluxioConf.isSetByUser(key);
  }

  @Override
  public Set<PropertyKey> keySet()
  {
    return mAlluxioConf.keySet();
  }

  @Override
  public Set<PropertyKey> userKeySet()
  {
    return mAlluxioConf.userKeySet();
  }

  @Override
  public String getString(PropertyKey key)
  {
    return mAlluxioConf.getString(key);
  }

  @Override
  public int getInt(PropertyKey key)
  {
    return mAlluxioConf.getInt(key);
  }

  @Override
  public long getLong(PropertyKey key) {
    return mAlluxioConf.getLong(key);
  }

  @Override
  public double getDouble(PropertyKey key)
  {
    return mAlluxioConf.getDouble(key);
  }

  @Override
  public boolean getBoolean(PropertyKey key)
  {
    return mAlluxioConf.getBoolean(key);
  }

  @Override
  public List<String> getList(PropertyKey key)
  {
    return mAlluxioConf.getList(key);
  }

  @Override
  public <T extends Enum<T>> T getEnum(PropertyKey key, Class<T> enumType)
  {
    return mAlluxioConf.getEnum(key, enumType);
  }

  @Override
  public long getBytes(PropertyKey key)
  {
    return mAlluxioConf.getBytes(key);
  }

  @Override
  public long getMs(PropertyKey key)
  {
    return mAlluxioConf.getMs(key);
  }

  @Override
  public Duration getDuration(PropertyKey key)
  {
    return mAlluxioConf.getDuration(key);
  }

  @Override
  public <T> Class<T> getClass(PropertyKey key)
  {
    return mAlluxioConf.getClass(key);
  }

  @Override
  public Map<String, Object> getNestedProperties(PropertyKey prefixKey)
  {
    return mAlluxioConf.getNestedProperties(prefixKey);
  }

  @Override
  public AlluxioProperties copyProperties()
  {
    return mAlluxioConf.copyProperties();
  }

  @Override
  public Source getSource(PropertyKey key)
  {
    return mAlluxioConf.getSource(key);
  }

  @Override
  public Map<String, Object> toMap(ConfigurationValueOptions opts)
  {
    return mAlluxioConf.toMap(opts);
  }

  @Override
  public void validate()
  {
    mAlluxioConf.validate();
  }

  @Override
  public boolean clusterDefaultsLoaded()
  {
    return mAlluxioConf.clusterDefaultsLoaded();
  }
}
