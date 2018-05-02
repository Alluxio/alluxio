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

import alluxio.master.meta.conf.ConfigRecorder;
import alluxio.wire.ConfigProperty;

import java.util.List;

/**
 * This class is responsible for checking server-side configuration.
 */
public final class ServerConfigurationChecker {
  private ConfigRecorder mMasterConfigRecorder;
  private ConfigRecorder mWorkerConfigRecorder;

  /**
   * Constructs a new {@link ServerConfigurationChecker}.
   */
  public ServerConfigurationChecker() {
    init();
  }

  /**
   * Inits when this master starts Alluxio master process or gets leadership.
   */
  public void init() {
    mMasterConfigRecorder = new ConfigRecorder();
    mWorkerConfigRecorder = new ConfigRecorder();
  }

  /**
   * Registers new configuration information.
   *
   * @param id the master/worker id
   * @param configList the configuration of this master/worker
   * @param isMaster whether this node is a master
   */
  public void registerNewConf(long id, List<ConfigProperty> configList, boolean isMaster) {
    if (isMaster) {
      mMasterConfigRecorder.registerNewConf(id, configList);
    } else {
      mWorkerConfigRecorder.registerNewConf(id, configList);
    }
  }

  /**
   * Removes the configuration of a master/worker from ConfMap to LostConfMap.
   *
   * @param id the master/worker id
   * @param isMaster whether this node is a master
   */
  public void removeConf(Long id, boolean isMaster) {
    if (isMaster) {
      mMasterConfigRecorder.removeConf(id);
    } else {
      mWorkerConfigRecorder.removeConf(id);
    }
  }

  /**
   * Adds the configuration of a master/worker from LostConfMap to ConfMap.
   *
   * @param id the master/worker id
   * @param isMaster whether this node is a master
   */
  public void addConf(Long id, boolean isMaster) {
    if (isMaster) {
      mMasterConfigRecorder.addConf(id);
    } else {
      mWorkerConfigRecorder.addConf(id);
    }
  }
}
