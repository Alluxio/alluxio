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

package alluxio.worker.job;

import alluxio.master.MasterClientConfig;
import alluxio.master.MasterInquireClient.Factory;

/**
 * Extension of MasterClientConfig with defaults that make sense for job master clients.
 */
public class JobMasterClientConfig extends MasterClientConfig {

  /**
   * @return a master client configuration with default values
   */
  public static JobMasterClientConfig defaults() {
    JobMasterClientConfig conf = new JobMasterClientConfig();
    conf.withMasterInquireClient(Factory.createForJobMaster());
    return conf;
  }
}
