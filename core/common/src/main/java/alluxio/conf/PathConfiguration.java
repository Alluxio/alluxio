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

package alluxio.conf;

import alluxio.AlluxioURI;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class used to resolve path level configuration.
 *
 * Preference for the value for a property follows:
 * 1. matched path level configuration;
 * 2. cluster level configuration.
 */
public final class PathConfiguration implements AlluxioConfiguration {
  private final AlluxioConfiguration mClusterConf;
  private final Map<String, AlluxioConfiguration> mPathConf;
  private final AlluxioURI mPath;
  private final Filter mFilter;

  /**
   * Filter out the path level configuration for a specific key.
   */
  private interface Filter {
    AlluxioConfiguration filter(PropertyKey key);
  }

  private final class PathFilter implements Filter {
    @Override
    public AlluxioConfiguration filter(PropertyKey key) {
      AlluxioConfiguration conf = mPathConf.get(mPath.getPath());
      if (conf != null && conf.isSet(key)) {
        return conf;
      }
      return mClusterConf;
    }
  }

  /**
   * Constructs a new PathConfiguration.
   *
   * @param clusterConf cluster level configuration
   * @param pathConf path level configuration
   * @param path the specific path this configuration is for
   */
  public PathConfiguration(AlluxioConfiguration clusterConf,
      Map<String, AlluxioConfiguration> pathConf, AlluxioURI path) {
    mClusterConf = clusterConf;
    mPathConf = pathConf;
    mPath = path;
    mFilter = new PathFilter();
  }

  @Override
  public String get(PropertyKey key) {
    return mFilter.filter(key).get(key);
  }

  @Override
  public String get(PropertyKey key, ConfigurationValueOptions options) {
    return mFilter.filter(key).get(key, options);
  }

  @Override
  public boolean isSet(PropertyKey key) {
    return mFilter.filter(key).isSet(key);
  }

  @Override
  public Set<PropertyKey> keySet() {
    return mClusterConf.keySet();
  }

  @Override
  public int getInt(PropertyKey key) {
    return mFilter.filter(key).getInt(key);
  }

  @Override
  public long getLong(PropertyKey key) {
    return mFilter.filter(key).getLong(key);
  }

  @Override
  public double getDouble(PropertyKey key) {
    return mFilter.filter(key).getDouble(key);
  }

  @Override
  public float getFloat(PropertyKey key) {
    return mFilter.filter(key).getFloat(key);
  }

  @Override
  public boolean getBoolean(PropertyKey key) {
    return mFilter.filter(key).getBoolean(key);
  }

  @Override
  public List<String> getList(PropertyKey key, String delimiter) {
    return mFilter.filter(key).getList(key, delimiter);
  }

  @Override
  public <T extends Enum<T>> T getEnum(PropertyKey key, Class<T> enumType) {
    return mFilter.filter(key).getEnum(key, enumType);
  }

  @Override
  public long getBytes(PropertyKey key) {
    return mFilter.filter(key).getBytes(key);
  }

  @Override
  public long getMs(PropertyKey key) {
    return mFilter.filter(key).getMs(key);
  }

  @Override
  public Duration getDuration(PropertyKey key) {
    return mFilter.filter(key).getDuration(key);
  }

  @Override
  public <T> Class<T> getClass(PropertyKey key) {
    return mFilter.filter(key).getClass(key);
  }

  @Override
  public Map<String, String> getNestedProperties(PropertyKey prefixKey) {
    return mFilter.filter(prefixKey).getNestedProperties(prefixKey);
  }

  @Override
  public AlluxioProperties copyProperties() {
    AlluxioProperties properties = mClusterConf.copyProperties();
    properties.forEach((key, value) ->
        properties.set(key, mFilter.filter(key).get(key)));
    return properties;
  }

  @Override
  public Source getSource(PropertyKey key) {
    return mFilter.filter(key).getSource(key);
  }

  @Override
  public Map<String, String> toMap(ConfigurationValueOptions opts) {
    Map<String, String> map = new HashMap<>();
    keySet().forEach(key ->
        map.put(key.getName(), mFilter.filter(key).get(key, opts)));
    return map;
  }

  @Override
  public void validate() {
    mClusterConf.validate();
    for (AlluxioConfiguration conf : mPathConf.values()) {
      conf.validate();
    }
  }

  @Override
  public boolean clusterDefaultsLoaded() {
    return mClusterConf.clusterDefaultsLoaded();
  }
}
