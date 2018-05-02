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
import java.util.List;
import java.util.Map;

/**
 * This class is responsible for containing server-side configurations.
 */
public class ConfigRecorder {
  /** Stores the configuration of a master/worker with Long id. */
  private Map<Long, List<ConfigProperty>> mConfMap;
  /** Stores the configuration of a lost master/worker with Long id. */
  private Map<Long, List<ConfigProperty>> mLostConfMap;

  /**
   * Constructs a new {@link ConfigRecorder}.
   */
  public ConfigRecorder() {
    mConfMap = new HashMap<>();
    mLostConfMap = new HashMap<>();
  }

  /**
   * Registers new configuration information.
   *
   * @param id the master/worker id
   * @param configList the configuration of this master/worker
   */
  public synchronized void registerNewConf(long id, List<ConfigProperty> configList) {
    mConfMap.put(id, configList);
    mLostConfMap.remove(id);
  }

  /**
   * Removes the configuration of a master/worker from ConfMap to LostConfMap
   * when detecting a live node becomes lost.
   *
   * @param id the master/worker id
   */
  public synchronized void detectNodeLost(long id) {
    List<ConfigProperty> configList = mConfMap.get(id);
    if (configList != null) {
      mLostConfMap.put(id, configList);
      mConfMap.remove(id);
    }
  }

  /**
   * Adds the configuration of a master/worker from LostConfMap to ConfMap
   * when a lost node is found.
   *
   * @param id the master/worker id
   */
  public synchronized void lostNodeFound(long id) {
    List<ConfigProperty> configList = mLostConfMap.get(id);
    if (configList != null) {
      mConfMap.put(id, configList);
      mLostConfMap.remove(id);
    }
  }

  /**
   * @return the configuration map of live masters or workers
   */
  public synchronized Map<Long, List<ConfigProperty>> getConfMap() {
    return mConfMap;
  }
}
