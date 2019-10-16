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

import alluxio.ClientContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.master.MasterClientContextBuilder;
import alluxio.master.MasterInquireClient;

/**
 * A builder for instances of {@link JobMasterClientContext}.
 */
public class JobMasterClientContextBuilder extends MasterClientContextBuilder {

  private MasterInquireClient mConfMasterInquireClient;

  /**
   * Creates a builder with the given {@link AlluxioConfiguration}.
   *
   * @param context Alluxio configuration
   */
  JobMasterClientContextBuilder(ClientContext context) {
    super(context);
  }

  /**
   * Builds the configuration, creating an instance of {@link MasterInquireClient} if none is
   * specified.
   *
   * @return a {@link JobMasterClientContext}
   */
  @Override
  public JobMasterClientContext build() {
    if (mMasterInquireClient == null) {
      mMasterInquireClient = MasterInquireClient.Factory.createForJobMaster(
          mContext.getClusterConf(), mContext.getUserState());
    }
    if (mConfMasterInquireClient == null) {
      mConfMasterInquireClient = MasterInquireClient.Factory.create(
              mContext.getClusterConf(), mContext.getUserState());
    }
    return new JobMasterClientContext(mContext, mMasterInquireClient, mConfMasterInquireClient);
  }
}
