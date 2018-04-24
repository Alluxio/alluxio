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

import javax.security.auth.Subject;

/**
 * Configuration for constructing an Alluxio master client.
 */
public class MasterClientConfig {
  private Subject mSubject;
  private MasterInquireClient mMasterInquireClient;

  /**
   * @return a master client configuration with default values
   */
  public static MasterClientConfig defaults() {
    return new MasterClientConfig().withMasterInquireClient(MasterInquireClient.Factory.create());
  }

  /**
   * @param subject a subject
   * @return the updated config
   */
  public MasterClientConfig withSubject(Subject subject) {
    mSubject = subject;
    return this;
  }

  /**
   * @param masterInquireClient a master inquire client
   * @return the updated config
   */
  public MasterClientConfig withMasterInquireClient(MasterInquireClient masterInquireClient) {
    mMasterInquireClient = masterInquireClient;
    return this;
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
}
