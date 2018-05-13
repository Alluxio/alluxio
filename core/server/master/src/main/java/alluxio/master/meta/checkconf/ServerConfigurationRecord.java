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
import alluxio.wire.Address;
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
  /** Map from a node address to its configuration. */
  private final Map<Address, List<ConfigRecord>> mConfMap;
  /** Set that contains the addresses of lost nodes. */
  private final Set<Address> mLostNodes;

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
   * @param address the node address
   * @param configList the configuration of this node
   */
  public synchronized void registerNewConf(Address address, List<ConfigProperty> configList) {
    Preconditions.checkNotNull(address, "address should not be null");
    Preconditions.checkNotNull(configList, "configuration list should not be null");
    // Instead of recording property name, we record property key.
    mConfMap.put(address, configList.stream().map(c -> new ConfigRecord()
        .setKey(PropertyKey.fromString(c.getName())).setSource(c.getSource())
        .setValue(c.getValue())).collect(Collectors.toList()));
    mLostNodes.remove(address);
  }

  /**
   * Updates configuration when a live node becomes lost.
   *
   * @param address the node address
   */
  public synchronized void handleNodeLost(Address address) {
    Preconditions.checkNotNull(address, "address should not be null");
    mLostNodes.add(address);
  }

  /**
   * Updates configuration when a lost node is found.
   *
   * @param address the node address
   */
  public synchronized void lostNodeFound(Address address) {
    Preconditions.checkNotNull(address, "address should not be null");
    mLostNodes.remove(address);
  }

  /**
   * @return the configuration map of live nodes
   */
  public synchronized Map<Address, List<ConfigRecord>> getConfMap() {
    Map<Address, List<ConfigRecord>> map = new HashMap<>();
    for (Map.Entry<Address, List<ConfigRecord>> entry : mConfMap.entrySet()) {
      Address address = entry.getKey();
      if (!mLostNodes.contains(address)) {
        map.put(address, entry.getValue());
      }
    }
    return map;
  }
}
