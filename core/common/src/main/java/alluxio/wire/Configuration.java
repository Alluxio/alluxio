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

import alluxio.conf.Source;
import alluxio.grpc.ConfigProperties;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GetConfigurationPResponse;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents cluster level and path level configuration returned by meta master.
 */
@ThreadSafe
public final class Configuration {
  /** List of cluster level properties. */
  private final List<Property> mClusterConf;
  /** Map from path to path level properties. */
  private final Map<String, List<Property>> mPathConf;
  /** Cluster configuration hash. */
  private final String mClusterConfHash;
  /** Path configuration hash. */
  private final String mPathConfHash;

  /**
   * @return new configuration builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Configuration builder.
   */
  @NotThreadSafe
  public static final class Builder {
    private List<Property> mClusterConf = new ArrayList<>();
    private Map<String, List<Property>> mPathConf = new HashMap<>();
    private String mClusterConfHash;
    private String mPathConfHash;

    /**
     * Adds a cluster level property.
     *
     * @param name property name
     * @param value property value
     * @param source property source
     */
    public void addClusterProperty(String name, @Nullable String value, Source source) {
      mClusterConf.add(new Property(name, value, source));
    }

    /**
     * Adds a path level property.
     *
     * @param path the path
     * @param name property name
     * @param value property value
     */
    public void addPathProperty(String path, String name, String value) {
      mPathConf.computeIfAbsent(path, k -> new ArrayList<Property>()).add(
          new Property(name, value, Source.PATH_DEFAULT));
    }

    /**
     * Sets hash of path configurations.
     *
     * @param hash the hash
     */
    public void setClusterConfHash(String hash) {
      Preconditions.checkNotNull(hash, "hash");
      mClusterConfHash = hash;
    }

    /**
     * Sets hash of path configurations.
     *
     * @param hash the hash
     */
    public void setPathConfHash(String hash) {
      Preconditions.checkNotNull(hash, "hash");
      mPathConfHash = hash;
    }

    /**
     * @return a newly constructed configuration
     */
    public Configuration build() {
      return new Configuration(mClusterConf, mPathConf, mClusterConfHash, mPathConfHash);
    }
  }

  private Configuration(List<Property> clusterConf, Map<String, List<Property>> pathConf,
      String clusterConfHash, String pathConfHash) {
    mClusterConf = clusterConf;
    mPathConf = pathConf;
    mClusterConfHash = clusterConfHash;
    mPathConfHash = pathConfHash;
  }

  private Configuration(GetConfigurationPResponse conf) {
    mClusterConf = conf.getClusterConfigsList().stream().map(Property::fromProto)
        .collect(Collectors.toList());

    Map<String, ConfigProperties> pathConf = conf.getPathConfigsMap();
    mPathConf = new HashMap<>(pathConf.size());
    pathConf.forEach((path, prop) -> {
      List<Property> properties = new ArrayList<>(prop.getPropertiesCount());
      prop.getPropertiesList().forEach(p -> properties.add(Property.fromProto(p)));
      mPathConf.put(path, properties);
    });

    mClusterConfHash = conf.getClusterConfigHash();
    mPathConfHash = conf.getPathConfigHash();
  }

  /**
   * @param conf the grpc representation of configuration
   * @return the wire representation of the proto response
   */
  public static Configuration fromProto(GetConfigurationPResponse conf) {
    return new Configuration(conf);
  }

  /**
   * @return the proto representation
   */
  public GetConfigurationPResponse toProto() {
    GetConfigurationPResponse.Builder response = GetConfigurationPResponse.newBuilder();
    if (mClusterConf != null) {
      mClusterConf.forEach(property -> response.addClusterConfigs(property.toProto()));
    }
    if (mPathConf != null) {
      mPathConf.forEach((path, properties) -> {
        List<ConfigProperty> propertyList = properties.stream().map(Property::toProto)
            .collect(Collectors.toList());
        ConfigProperties configProperties = ConfigProperties.newBuilder()
            .addAllProperties(propertyList).build();
        response.putPathConfigs(path, configProperties);
      });
    }
    if (mClusterConfHash != null) {
      response.setClusterConfigHash(mClusterConfHash);
    }
    if (mPathConfHash != null) {
      response.setPathConfigHash(mPathConfHash);
    }
    return response.build();
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

  /**
   * @return cluster level configuration hash
   */
  public String getClusterConfHash() {
    return mClusterConfHash;
  }

  /**
   * @return path level configuration hash
   */
  public String getPathConfHash() {
    return mPathConfHash;
  }
}
