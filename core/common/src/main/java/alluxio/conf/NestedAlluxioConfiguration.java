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

import com.google.common.collect.ImmutableMap;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Configuration that wraps around another configuration
 * Priority for the value of a property follows:
 * 1. outer configuration
 * 2. inner configuration
 */
@ThreadSafe
public class NestedAlluxioConfiguration implements AlluxioConfiguration {
  /**
   * Runtime level configuration.
   */
  private final AlluxioConfiguration mOuterConf;
  /**
   * Default configuration.
   */
  private final AlluxioConfiguration mInnerConf;

  /**
   * Constructs a new instance with the specified references without copying the underlying
   * properties.
   *
   * @param outerConf the runtime level configuration to override
   * @param innerConf the default configuration
   */
  public NestedAlluxioConfiguration(AlluxioConfiguration outerConf,
      AlluxioConfiguration innerConf) {
    mOuterConf = outerConf;
    mInnerConf = innerConf;
  }

  private AlluxioConfiguration conf(PropertyKey key) {
    return mOuterConf.isSet(key) ? mOuterConf : mInnerConf;
  }

  @Override
  public Object get(PropertyKey key) {
    return conf(key).get(key);
  }

  @Override
  public Object get(PropertyKey key, ConfigurationValueOptions options) {
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
    return mInnerConf.keySet();
  }

  @Override
  public Set<PropertyKey> userKeySet() {
    return mOuterConf.keySet();
  }

  @Override
  public String getString(PropertyKey key) {
    return conf(key).getString(key);
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
  public boolean getBoolean(PropertyKey key) {
    return conf(key).getBoolean(key);
  }

  @Override
  public List<String> getList(PropertyKey key) {
    return conf(key).getList(key);
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
  public Map<String, Object> getNestedProperties(PropertyKey prefixKey) {
    return conf(prefixKey).getNestedProperties(prefixKey);
  }

  @Override
  public AlluxioProperties copyProperties() {
    AlluxioProperties properties = mInnerConf.copyProperties();
    for (PropertyKey key : keySet()) {
      properties.put(key, mOuterConf.get(key), Source.RUNTIME);
    }
    return properties;
  }

  @Override
  public Source getSource(PropertyKey key) {
    return conf(key).getSource(key);
  }

  @Override
  public Map<String, Object> toMap(ConfigurationValueOptions opts) {
    ImmutableMap.Builder<String, Object> map = ImmutableMap.builder();
    // Cannot use Collectors.toMap because we support null keys.
    keySet().forEach(key ->
        map.put(key.getName(), conf(key).getOrDefault(key, null, opts)));
    return map.build();
  }

  @Override
  public void validate() {
    new InstancedConfiguration(copyProperties()).validate();
  }

  @Override
  public boolean clusterDefaultsLoaded() {
    return mInnerConf.clusterDefaultsLoaded();
  }
}
