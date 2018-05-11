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

package alluxio.master.meta.checkconf;

import alluxio.PropertyKey;
import alluxio.wire.ConfigProperty;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is responsible for recording server-side configuration.
 */
public class ServerConfigurationRecord {
  /** Map from a node id to its configuration. */
  private final Map<String, List<ConfigRecord>> mConfMap;
  /** Set that contains the ids of lost nodes. */
  private final Set<String> mLostNodes;

  /**
   * Constructs a new {@link ServerConfigurationRecord}.
   */
  public ServerConfigurationRecord() {
    mConfMap = new HashMap<>();
    mLostNodes = new HashSet<>();
  }

  /**
   * Resets the default {@link ServerConfigurationRecord}.
   */
  public synchronized void reset() {
    mConfMap.clear();
    mLostNodes.clear();
  }

  /**
   * Registers new configuration information.
   *
   * @param hostname the node hostname
   * @param configList the configuration of this node
   */
  public synchronized void registerNewConf(String hostname, List<ConfigProperty> configList) {
    Preconditions.checkNotNull(hostname, "hostname should not be null");
    Preconditions.checkNotNull(configList, "configuration list should not be null");
    // Instead of recording property name, we record property key.
    mConfMap.put(hostname, configList.stream().map(c -> new ConfigRecord()
        .setKey(PropertyKey.fromString(c.getName())).setSource(c.getSource())
        .setValue(c.getValue())).collect(Collectors.toList()));
    mLostNodes.remove(hostname);
  }

  /**
   * Updates configuration when a live node becomes lost.
   *
   * @param hostname the node hostname
   */
  public synchronized void handleNodeLost(String hostname) {
    Preconditions.checkNotNull(hostname, "hostname should not be null");
    mLostNodes.add(hostname);
  }

  /**
   * Updates configuration when a lost node is found.
   *
   * @param hostname the node hostname
   */
  public synchronized void lostNodeFound(String hostname) {
    Preconditions.checkNotNull(hostname, "hostname should not be null");
    mLostNodes.remove(hostname);
  }

  /**
   * @return the configuration map of live nodes
   */
  public synchronized Map<String, List<ConfigRecord>> getConfMap() {
    Map<String, List<ConfigRecord>> map = new HashMap<>();
    for (Map.Entry<String, List<ConfigRecord>> entry : mConfMap.entrySet()) {
      String hostname = entry.getKey();
      if (!mLostNodes.contains(hostname)) {
        map.put(hostname, entry.getValue());
      }
    }
    return map;
  }
}
