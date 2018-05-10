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

package alluxio.master.meta.checkConf;

import alluxio.PropertyKey;
import alluxio.wire.ConfigProperty;

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
  private final Map<Long, List<ConfigRecord>> mConfMap;
  /** Set that contains the ids of lost nodes. */
  private final Set<Long> mLostNodes;

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
   * @param id the node id
   * @param configList the configuration of this node
   */
  public synchronized void registerNewConf(long id, List<ConfigProperty> configList) {
    mConfMap.put(id, configList.stream().map(c -> new ConfigRecord()
        .setKey(PropertyKey.fromString(c.getName())).setSource(c.getSource()).setValue(c.getValue()))
        .collect(Collectors.toList()));
    mLostNodes.remove(id);
  }

  /**
   * Updates configuration when a live node becomes lost.
   *
   * @param id the node id
   */
  public synchronized void handleNodeLost(long id) {
    mLostNodes.add(id);
  }

  /**
   * Updates configuration when a lost node is found.
   *
   * @param id the node id
   */
  public synchronized void lostNodeFound(long id) {
    mLostNodes.remove(id);
  }

  /**
   * @return the configuration map of live nodes
   */
  public synchronized Map<Long, List<ConfigRecord>> getConfMap() {
    Map<Long, List<ConfigRecord>> map = new HashMap<>();
    for (Map.Entry<Long, List<ConfigRecord>> entry : mConfMap.entrySet()) {
      Long id = entry.getKey();
      if (!mLostNodes.contains(id)) {
        map.put(id, entry.getValue());
      }
    }
    return map;
  }
}
