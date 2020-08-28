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
import alluxio.conf.ConfigurationValueOptions;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Configuration for a specific Alluxio path.
 *
 * Priority for the value of a property follows:
 * 1. matched path level configuration;
 * 2. cluster level configuration.
 */
@ThreadSafe
public final class SpecificPathConfiguration implements AlluxioConfiguration {
  /**
   * Cluster level configuration.
   */
  private final AlluxioConfiguration mClusterConf;
  /**
   * Path level configuration.
   */
  private final PathConfiguration mPathConf;
  /**
   * The specific path.
   */
  private final AlluxioURI mPath;

  /**
   * Constructs a new instance with the specified references without copying the underlying
   * properties.
   *
   * @param clusterConf the cluster level configuration
   * @param pathConf the path level configuration
   * @param path the specific Alluxio path
   */
  public SpecificPathConfiguration(AlluxioConfiguration clusterConf, PathConfiguration pathConf,
      AlluxioURI path) {
    mClusterConf = clusterConf;
    mPathConf = pathConf;
    mPath = path;
  }

  private AlluxioConfiguration conf(PropertyKey key) {
    return mPathConf.getConfiguration(mPath, key).orElse(mClusterConf);
  }

  @Override
  public String get(PropertyKey key) {
    return conf(key).get(key);
  }

  @Override
  public String get(PropertyKey key, ConfigurationValueOptions options) {
    return conf(key).get(key, options);
  }

  @Override
  public boolean isSet(PropertyKey key) {
    return conf(key).isSet(key);
  }

  @Override
  public boolean isSetByUser(PropertyKey key) {
    return conf(key).isSetByUser(key);
  }

  @Override
  public Set<PropertyKey> keySet() {
    return mClusterConf.keySet();
  }

  @Override
  public Set<PropertyKey> userKeySet() {
    return mPathConf.getPropertyKeys(mPath);
  }

  @Override
  public int getInt(PropertyKey key) {
    return conf(key).getInt(key);
  }

  @Override
  public long getLong(PropertyKey key) {
    return conf(key).getLong(key);
  }

  @Override
  public double getDouble(PropertyKey key) {
    return conf(key).getDouble(key);
  }

  @Override
  public float getFloat(PropertyKey key) {
    return conf(key).getFloat(key);
  }

  @Override
  public boolean getBoolean(PropertyKey key) {
    return conf(key).getBoolean(key);
  }

  @Override
  public List<String> getList(PropertyKey key, String delimiter) {
    return conf(key).getList(key, delimiter);
  }

  @Override
  public <T extends Enum<T>> T getEnum(PropertyKey key, Class<T> enumType) {
    return conf(key).getEnum(key, enumType);
  }

  @Override
  public long getBytes(PropertyKey key) {
    return conf(key).getBytes(key);
  }

  @Override
  public long getMs(PropertyKey key) {
    return conf(key).getMs(key);
  }

  @Override
  public Duration getDuration(PropertyKey key) {
    return conf(key).getDuration(key);
  }

  @Override
  public <T> Class<T> getClass(PropertyKey key) {
    return conf(key).getClass(key);
  }

  @Override
  public Map<String, String> getNestedProperties(PropertyKey prefixKey) {
    return conf(prefixKey).getNestedProperties(prefixKey);
  }

  @Override
  public AlluxioProperties copyProperties() {
    AlluxioProperties properties = mClusterConf.copyProperties();
    for (PropertyKey key : keySet()) {
      mPathConf.getConfiguration(mPath, key).ifPresent(
          config -> properties.put(key, config.get(key), Source.PATH_DEFAULT));
    }
    return properties;
  }

  @Override
  public Source getSource(PropertyKey key) {
    return conf(key).getSource(key);
  }

  @Override
  public Map<String, String> toMap(ConfigurationValueOptions opts) {
    Map<String, String> map = new HashMap<>();
    // Cannot use Collectors.toMap because we support null keys.
    keySet().forEach(key ->
        map.put(key.getName(), conf(key).getOrDefault(key, null, opts)));
    return map;
  }

  @Override
  public void validate() {
    new InstancedConfiguration(copyProperties()).validate();
  }

  @Override
  public boolean clusterDefaultsLoaded() {
    return mClusterConf.clusterDefaultsLoaded();
  }
}
