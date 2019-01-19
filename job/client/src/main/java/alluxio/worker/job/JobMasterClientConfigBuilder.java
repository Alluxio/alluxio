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

import alluxio.conf.AlluxioConfiguration;
import alluxio.master.MasterClientConfigBuilder;
import alluxio.master.MasterInquireClient;

/**
 * A builder for instances of {@link JobMasterClientConfig}.
 */
public class JobMasterClientConfigBuilder extends MasterClientConfigBuilder {

  /**
   * Creates a builder with the given {@link AlluxioConfiguration}.
   *
   * @param alluxioConf Alluxio configuration
   */
  JobMasterClientConfigBuilder(AlluxioConfiguration alluxioConf) {
    super(alluxioConf);
  }

  /**
   * Builds the configuration, creating an instance of {@link MasterInquireClient} if none is
   * specified.
   *
   * @return a {@link JobMasterClientConfig}
   */
  @Override
  public JobMasterClientConfig build() {
    if (mMasterInquireClient == null) {
      mMasterInquireClient = MasterInquireClient.Factory.createForJobMaster(mAlluxioConf);
    }
    return new JobMasterClientConfig(mAlluxioConf, mMasterInquireClient, mSubject);
  }
}
