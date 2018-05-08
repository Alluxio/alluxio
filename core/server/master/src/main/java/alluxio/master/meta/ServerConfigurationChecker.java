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

import java.util.ArrayList;
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
  private final Map<Long, List<ConfigProperty>> mConfMap;
  /** Set that contains the ids of lost nodes. */
  private final Set<Long> mLostNodes;

  /**
   * Constructs a new {@link ServerConfigurationChecker}.
   */
  public ServerConfigurationChecker() {
    mConfMap = new HashMap<>();
    mLostNodes = new HashSet<>();
  }

  /**
   * Resets the default {@link ServerConfigurationChecker}.
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

  /**
   * @return the configuration error map.
   */
  public synchronized Map<String, Map<String, List<Long>>> getConfErrors() {
    // The confValues and confErrors maps are of format Map<Property Name, Map<Property Value, List<Id>>>

    // Record all the property names and its values and ids belong to those values.
    Map<String, Map<String, List<Long>>> confValues = new HashMap<>();

    // If one property has more than one value, it is defined as a configuration error
    // and will be put into the confErrorsMap.
    Map<String, Map<String, List<Long>>> confErrors = new HashMap<>();

    // Fill the confValues from mConfMap
    for (Map.Entry<Long, List<ConfigProperty>> entry : mConfMap.entrySet()) {
      Long id = entry.getKey();
      for (ConfigProperty configProperty : entry.getValue()) {
        String name = configProperty.getName();
        String value = configProperty.getValue();
        confValues.putIfAbsent(name, new HashMap<>());
        Map<String, List<Long>> values = confValues.get(name);
        List<Long> ids = values.getOrDefault(value, new ArrayList<>());
        ids.add(id);
        values.put(value, ids);
      }
    }


    // Fill the confErrors from confValues
    for (Map.Entry<String, Map<String, List<Long>>> entry : confValues.entrySet()) {
      if (entry.getValue().size() >= 2) {
        confErrors.put(entry.getKey(), entry.getValue());
      }
    }

    return confErrors;
  }
}
