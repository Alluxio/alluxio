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

package alluxio.master.meta;

import alluxio.wire.ConfigProperty;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is responsible for checking server-side configuration.
 */
public class ServerConfigurationChecker {
  /** Map from a node id to its configuration. */
  private Map<Long, List<ConfigProperty>> mConfMap;
  /** Set that contains the ids of lost nodes. */
  private Set<Long> mLostNodes;

  /**
   * Constructs a new {@link ServerConfigurationChecker}.
   */
  public ServerConfigurationChecker() {
    init();
  }

  /**
   * Initializes the default {@link ServerConfigurationChecker}.
   */
  public synchronized void init() {
    mConfMap = new HashMap<>();
    mLostNodes = new HashSet<>();
  }

  /**
   * Registers new configuration information.
   *
   * @param id the node id
   * @param configList the configuration of this node
   */
  public synchronized void registerNewConf(long id, List<ConfigProperty> configList) {
    mConfMap.put(id, configList);
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
  public synchronized Map<Long, List<ConfigProperty>> getConfMap() {
    Map<Long, List<ConfigProperty>> map = new HashMap<>();
    for (Map.Entry<Long, List<ConfigProperty>> entry : mConfMap.entrySet()) {
      Long id = entry.getKey();
      if (!mLostNodes.contains(id)) {
        map.put(id, entry.getValue());
      }
    }
    return map;
  }
}
