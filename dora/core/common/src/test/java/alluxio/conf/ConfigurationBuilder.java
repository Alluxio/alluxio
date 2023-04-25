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

/**
 * Class for building an Alluxio configuration during tests. The built configuration does not load
 * properties from alluxio-site.properties.
 */
public class ConfigurationBuilder {
  private final AlluxioProperties mProperties = new AlluxioProperties();

  /**
   * @param key the property key to set
   * @param value the value to set
   * @return the updated configuration builder
   */
  public ConfigurationBuilder setProperty(PropertyKey key, Object value) {
    mProperties.put(key, value.toString(), Source.RUNTIME);
    return this;
  }

  /**
   * @return a constructed configuration
   */
  public InstancedConfiguration build() {
    return new InstancedConfiguration(mProperties);
  }
}
