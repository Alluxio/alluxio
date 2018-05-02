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

package alluxio.master.meta.conf;

import alluxio.wire.ConfigProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is responsible for containing server-side configurations.
 */
public class ConfigRecorder {
  /** Map from ids to Object lockers. */
  private ConcurrentHashMap<Long, Object> mLockers;
  /** Stores the configuration of a master/worker with Long id. */
  private Map<Long, List<ConfigProperty>> mConfMap;
  /** Stores the configuration of a lost master/worker with Long id. */
  private Map<Long, List<ConfigProperty>> mLostConfMap;

  /**
   * Constructs a new {@link ConfigRecorder}.
   */
  public ConfigRecorder() {
    mLockers = new ConcurrentHashMap<>();
    mConfMap = new HashMap<>();
    mLostConfMap = new HashMap<>();
  }

  /**
   * Registers new configuration information.
   *
   * @param id the master/worker id
   * @param configList the configuration of this master/worker
   */
  public void registerNewConf(Long id, List<ConfigProperty> configList) {
    mLockers.putIfAbsent(id, new Object());
    synchronized (mLockers.get(id)) {
      mConfMap.put(id, configList);
      mLostConfMap.remove(id);
    }
  }

  /**
   * Removes the configuration of a master/worker from ConfMap to LostConfMap.
   *
   * @param id the master/worker id
   */
  public void removeConf(Long id) {
    List<ConfigProperty> configList = mConfMap.get(id);
    if (configList != null) {
      mLockers.putIfAbsent(id, new Object());
      synchronized (mLockers.get(id)) {
        mLostConfMap.put(id, configList);
        mConfMap.remove(id);
      }
    }
  }

  /**
   * Adds the configuration of a master/worker from LostConfMap to ConfMap.
   *
   * @param id the master/worker id
   */
  public void addConf(Long id) {
    List<ConfigProperty> configList = mLostConfMap.get(id);
    if (configList != null) {
      mLockers.putIfAbsent(id, new Object());
      synchronized (mLockers.get(id)) {
        mConfMap.put(id, configList);
        mLostConfMap.remove(id);
      }
    }
  }
}
