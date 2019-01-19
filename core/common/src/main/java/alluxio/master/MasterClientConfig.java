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

import alluxio.conf.AlluxioConfiguration;

import javax.security.auth.Subject;

/**
 * Configuration for constructing an Alluxio master client.
 */
public class MasterClientConfig {
  private Subject mSubject;
  private MasterInquireClient mMasterInquireClient;
  private AlluxioConfiguration mAlluxioConf;

  // Prevent outside instantiation
  protected MasterClientConfig(AlluxioConfiguration alluxioConf,
      MasterInquireClient masterInquireClient, Subject subject) {
    mAlluxioConf = alluxioConf;
    mMasterInquireClient = masterInquireClient;
    mSubject = subject;
  }

  /**
   * Create an instance of a {@link MasterClientConfigBuilder} which can be used to obtain and
   * instance of {@link MasterClientConfig}.
   *
   * @param alluxioConf The alluxio configuration to base the client config on
   * @return A builder for a {@link MasterClientConfig}
   */
  public static MasterClientConfigBuilder newBuilder(AlluxioConfiguration alluxioConf) {
    return new MasterClientConfigBuilder(alluxioConf);
  }

  /**
   * @return the subject
   */
  public Subject getSubject() {
    return mSubject;
  }

  /**
   * @return the master inquire client
   */
  public MasterInquireClient getMasterInquireClient() {
    return mMasterInquireClient;
  }

  /**
   * @return the client configuration
   */
  public AlluxioConfiguration getConfiguration() {
    return mAlluxioConf;
  }
}
