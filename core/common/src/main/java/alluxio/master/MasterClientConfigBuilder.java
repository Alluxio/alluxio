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

import com.google.common.base.Preconditions;

import javax.security.auth.Subject;

/**
 * This class can be used to obtain instances of a {@link MasterClientConfig}. This is the
 * preferred method of creating master client configurations.
 */
public class MasterClientConfigBuilder {
  protected Subject mSubject;
  protected MasterInquireClient mMasterInquireClient;
  protected AlluxioConfiguration mAlluxioConf;

  /**
   * Create an instance of a {@link MasterClientConfigBuilder}.
   *
   * @param alluxioConf The Alluxio configuration to base the config on
   */
  public MasterClientConfigBuilder(AlluxioConfiguration alluxioConf) {
    mAlluxioConf = Preconditions.checkNotNull(alluxioConf);
  }

  /**
   * Set the {@link MasterInquireClient} that the config will use.
   *
   * @param masterInquireClient the master inquire client
   * @return the builder
   */
  public MasterClientConfigBuilder setMasterInquireClient(
      MasterInquireClient masterInquireClient) {
    mMasterInquireClient = masterInquireClient;
    return this;
  }

  /**
   * Set the {@link Subject} that the config will use.
   *
   * @param subject the subject
   * @return the builder
   */
  public MasterClientConfigBuilder setSubject(Subject subject) {
    mSubject = subject;
    return this;
  }

  /**
   * @return an instance of {@link MasterClientConfig}
   */
  public MasterClientConfig build() {
    if (mMasterInquireClient == null) {
      mMasterInquireClient = MasterInquireClient.Factory.create(mAlluxioConf);
    }
    return new MasterClientConfig(mAlluxioConf, mMasterInquireClient, mSubject);
  }
}
