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

import alluxio.conf.PropertyKey;
import alluxio.grpc.ConfigProperty;
import alluxio.wire.Address;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is responsible for recording server-side configuration.
 */
@ThreadSafe
public class ServerConfigurationStore {
  /** Map from a node address to its configuration. */
  private final Map<Address, List<ConfigRecord>> mConfMap;
  /** Set that contains the addresses of lost nodes. */
  private final Set<Address> mLostNodes;

  /** Listeners to call when this store has any changes. */
  private final List<Runnable> mChangeListeners = new ArrayList<>();

  /**
   * Constructs a new {@link ServerConfigurationStore}.
   */
  public ServerConfigurationStore() {
    mConfMap = new HashMap<>();
    mLostNodes = new HashSet<>();
  }

  /**
   * Resets the default {@link ServerConfigurationStore}.
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
        .setKey(toPropertyKey(c.getName())).setSource(c.getSource())
        .setValue(c.getValue())).collect(Collectors.toList()));
    mLostNodes.remove(address);
    for (Runnable function : mChangeListeners) {
      function.run();
    }
  }

  private static PropertyKey toPropertyKey(String name) {
    if (PropertyKey.isValid(name)) {
      return PropertyKey.fromString(name);
    } else {
      // Worker might have properties that the master doesn't yet know about, e.g. UFS specific
      // properties, or properties from a different version of Alluxio.
      return new PropertyKey.Builder(name).setIsBuiltIn(false).buildUnregistered();
    }
  }

  /**
   * Updates configuration when a live node becomes lost.
   *
   * @param address the node address
   */
  public synchronized void handleNodeLost(Address address) {
    Preconditions.checkNotNull(address, "address should not be null");
    mLostNodes.add(address);
    for (Runnable function : mChangeListeners) {
      function.run();
    }
  }

  /**
   * Updates configuration when a lost node is found.
   *
   * @param address the node address
   */
  public synchronized void lostNodeFound(Address address) {
    Preconditions.checkNotNull(address, "address should not be null");
    mLostNodes.remove(address);
    for (Runnable function : mChangeListeners) {
      function.run();
    }
  }

  /**
   * @return a copy of the configuration map of live nodes
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

  /**
   * @return the addresses of live nodes
   */
  public synchronized List<Address> getLiveNodeAddresses() {
    return mConfMap.keySet().stream()
        .filter(address -> !mLostNodes.contains(address)).collect(Collectors.toList());
  }

  /**
   * Registers callback functions to use when this store has any changes.
   *
   * @param function the function to register
   */
  public synchronized void registerChangeListener(Runnable function) {
    mChangeListeners.add(function);
  }
}
