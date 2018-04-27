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
 * This class is responsible for containing configurations and creating server configuration report.
 */
public final class ServerConfigurationReport {
  private static Map<Long, List<ConfigProperty>> sWorkerConfMap;
  private static Map<Long, List<ConfigProperty>> sLostWorkerConfMap;
  private static Map<Long, List<ConfigProperty>> sMasterConfMap;
  private static Map<Long, List<ConfigProperty>> sLostMasterConfMap;

  /**
   * Inits when this master start Alluxio master process or get leadership.
   */
  public static void init() {
    sWorkerConfMap = new HashMap<>();
    sLostWorkerConfMap = new HashMap<>();
    sMasterConfMap = new HashMap<>();
    sLostMasterConfMap = new HashMap<>();
  }

  /**
   * Registers new configuration information.
   *
   * @param id the master/worker id
   * @param configList the configuration of this master/worker
   * @param isMaster whether this node is a master
   */
  public static void registerNewConf(long id, List<ConfigProperty> configList, boolean isMaster) {
    if (isMaster) {
      sMasterConfMap.put(id, configList);
      sLostMasterConfMap.remove(id);
    } else {
      sWorkerConfMap.put(id, configList);
      sLostMasterConfMap.remove(id);
    }
  }

  /**
   * Removes the configuration of a master/worker from ConfMap to LostConfMap.
   *
   * @param id the master/worker id
   * @param isMaster whether this node is a master
   */
  public static void removeConf(long id, boolean isMaster) {
    if (isMaster) {
      sLostMasterConfMap.put(id, sMasterConfMap.get(id));
      sMasterConfMap.remove(id);
    } else {
      sLostWorkerConfMap.put(id, sWorkerConfMap.get(id));
      sWorkerConfMap.remove(id);
    }
  }

  /**
   * Adds the configuration of a master/worker from LostConfMap to ConfMap.
   *
   * @param id the master/worker id
   * @param isMaster whether this node is a master
   */
  public static void addConf(long id, boolean isMaster) {
    if (isMaster) {
      sMasterConfMap.put(id, sLostMasterConfMap.get(id));
      sLostMasterConfMap.remove(id);
    } else {
      sWorkerConfMap.put(id, sLostWorkerConfMap.get(id));
      sLostWorkerConfMap.remove(id);
    }
  }

  private ServerConfigurationReport() {} // prevent instantiation
}
