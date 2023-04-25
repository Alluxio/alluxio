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

/**
 * Configuration for constructing an Alluxio master client.
 */
public class MasterClientContext extends ClientContext {
  private final MasterInquireClient mMasterInquireClient;

  // Prevent outside instantiation
  protected MasterClientContext(ClientContext ctx, MasterInquireClient masterInquireClient) {
    super(ctx);
    mMasterInquireClient = masterInquireClient;
  }

  /**
   * Create an instance of a {@link MasterClientContextBuilder} which can be used to obtain and
   * instance of {@link MasterClientContext}.
   *
   * @param ctx The context to base
   * @return a builder for a {@link MasterClientContext}
   */
  public static MasterClientContextBuilder newBuilder(ClientContext ctx) {
    return new MasterClientContextBuilder(ctx);
  }

  /**
   * @return the master inquire client
   */
  public MasterInquireClient getMasterInquireClient() {
    return mMasterInquireClient;
  }

  /**
   * @return the master inquire client for configuration
   */
  public MasterInquireClient getConfMasterInquireClient() {
    return mMasterInquireClient;
  }
}
