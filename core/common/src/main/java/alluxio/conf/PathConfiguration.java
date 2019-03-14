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

import java.time.Duration;
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
  private final AlluxioConfiguration mConf;
  private final int mPathIndex;

  /**
   * Constructs a new PathConfiguration.
   *
   * It stores a reference to conf without copying properties.
   *
   * @param conf the Alluxio configuration
   * @param pathIndex the index of the path this configuration is for
   */
  public PathConfiguration(AlluxioConfiguration conf, int pathIndex) {
    mConf = conf;
    mPathIndex = pathIndex;
  }

  private PropertyKey resolve(PropertyKey key) {
    PropertyKey pathKey = PropertyKey.Template.PATH_PROPERTY.format(mPathIndex, key.toString());
    return mConf.isSet(pathKey) ? pathKey : key;
  }

  @Override
  public String get(PropertyKey key) {
    return mConf.get(resolve(key));
  }

  @Override
  public String get(PropertyKey key, ConfigurationValueOptions options) {
    return mConf.get(resolve(key), options);
  }

  @Override
  public boolean isSet(PropertyKey key) {
    return mConf.isSet(resolve(key));
  }

  @Override
  public Set<PropertyKey> keySet() {
    return mConf.keySet();
  }

  @Override
  public int getInt(PropertyKey key) {
    return mConf.getInt(resolve(key));
  }

  @Override
  public long getLong(PropertyKey key) {
    return mConf.getLong(resolve(key));
  }

  @Override
  public double getDouble(PropertyKey key) {
    return mConf.getDouble(resolve(key));
  }

  @Override
  public float getFloat(PropertyKey key) {
    return mConf.getFloat(resolve(key));
  }

  @Override
  public boolean getBoolean(PropertyKey key) {
    return mConf.getBoolean(resolve(key));
  }

  @Override
  public List<String> getList(PropertyKey key, String delimiter) {
    return mConf.getList(resolve(key), delimiter);
  }

  @Override
  public <T extends Enum<T>> T getEnum(PropertyKey key, Class<T> enumType) {
    return mConf.getEnum(resolve(key), enumType);
  }

  @Override
  public long getBytes(PropertyKey key) {
    return mConf.getBytes(resolve(key));
  }

  @Override
  public long getMs(PropertyKey key) {
    return mConf.getMs(resolve(key));
  }

  @Override
  public Duration getDuration(PropertyKey key) {
    return mConf.getDuration(resolve(key));
  }

  @Override
  public <T> Class<T> getClass(PropertyKey key) {
    return mConf.getClass(resolve(key));
  }

  @Override
  public Map<String, String> getNestedProperties(PropertyKey prefixKey) {
    return mConf.getNestedProperties(resolve(prefixKey));
  }

  @Override
  public AlluxioProperties copyProperties() {
    return mConf.copyProperties();
  }

  @Override
  public Source getSource(PropertyKey key) {
    return mConf.getSource(resolve(key));
  }

  @Override
  public Map<String, String> toMap(ConfigurationValueOptions opts) {
    return mConf.toMap(opts);
  }

  @Override
  public void validate() {
    mConf.validate();
  }

  @Override
  public boolean clusterDefaultsLoaded() {
    return mConf.clusterDefaultsLoaded();
  }
}
