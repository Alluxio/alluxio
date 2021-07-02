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

package alluxio.master;

import alluxio.ClientContext;

import com.google.common.base.Preconditions;

/**
 * This class can be used to obtain instances of a {@link MasterClientContext}. This is the
 * preferred method of creating master client configurations.
 */
public class MasterClientContextBuilder {
  protected ClientContext mContext;
  protected MasterInquireClient mMasterInquireClient;

  /**
   * Create an instance of a {@link MasterClientContextBuilder}.
   *
   * @param ctx The {@link ClientContext} to base the configuration on
   */
  public MasterClientContextBuilder(ClientContext ctx) {
    mContext = Preconditions.checkNotNull(ctx, "ctx");
  }

  /**
   * Set the {@link MasterInquireClient} that the config will use.
   *
   * @param masterInquireClient the master inquire client
   * @return the builder
   */
  public MasterClientContextBuilder setMasterInquireClient(
      MasterInquireClient masterInquireClient) {
    mMasterInquireClient = masterInquireClient;
    return this;
  }

  /**
   * @return an instance of {@link MasterClientContext}
   */
  public MasterClientContext build() {
    if (mMasterInquireClient == null) {
      mMasterInquireClient = MasterInquireClient.Factory.create(mContext.getClusterConf(),
          mContext.getUserState());
    }
    return new MasterClientContext(mContext, mMasterInquireClient);
  }
}
