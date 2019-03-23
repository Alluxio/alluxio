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

package alluxio.wire;

import alluxio.grpc.ConfigProperties;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GetConfigurationPResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents cluster level and path level configuration returned by meta master.
 */
public final class Configuration {
  /** List of cluster level properties. */
  private final List<Property> mClusterConf;
  /** Map from path to path level properties. */
  private final Map<String, List<Property>> mPathConf;

  private Configuration(GetConfigurationPResponse conf) {
    List<ConfigProperty> clusterConf = conf.getConfigsList();
    mClusterConf = new ArrayList<>(clusterConf.size());
    clusterConf.forEach(prop -> mClusterConf.add(Property.fromProto(prop)));

    Map<String, ConfigProperties> pathConf = conf.getPathConfigsMap();
    mPathConf = new HashMap<>(pathConf.size());
    pathConf.forEach((path, prop) -> {
      List<Property> properties = new ArrayList<>(prop.getPropertiesCount());
      prop.getPropertiesList().forEach(p -> properties.add(Property.fromProto(p)));
      mPathConf.put(path, properties);
    });
  }

  /**
   * @return the wire representation of the proto response
   */
  public static Configuration fromProto(GetConfigurationPResponse conf) {
    return new Configuration(conf);
  }

  /**
   * @return the internal cluster level configuration
   */
  public List<Property> getClusterConf() {
    return mClusterConf;
  }

  /**
   * @return the internal path level configuration
   */
  public Map<String, List<Property>> getPathConf() {
    return mPathConf;
  }
}
