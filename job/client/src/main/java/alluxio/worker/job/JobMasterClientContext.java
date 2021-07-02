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

import alluxio.master.MasterClientContext;
import alluxio.master.MasterInquireClient;

/**
 * Extension of MasterClientContext with defaults that make sense for job master clients.
 */
public class JobMasterClientContext extends MasterClientContext {

  private MasterInquireClient mConfMasterInquireClient;

  /**
   * Create a builder for {@link JobMasterClientContext}.
   *
   * @param context information used to connect to Alluxio
   * @return the builder for {@link JobMasterClientContext}
   */
  public static JobMasterClientContextBuilder newBuilder(ClientContext context) {
    return new JobMasterClientContextBuilder(context);
  }

  protected JobMasterClientContext(ClientContext context, MasterInquireClient masterInquireClient,
                                   MasterInquireClient confMasterInquireClient) {
    super(context, masterInquireClient);
    mConfMasterInquireClient = confMasterInquireClient;
  }

  @Override
  public MasterInquireClient getConfMasterInquireClient() {
    return mConfMasterInquireClient;
  }
}
