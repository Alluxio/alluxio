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

package alluxio;

import alluxio.exception.status.UnavailableException;
import alluxio.master.MasterInquireClient;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * The base class for master clients.
 */
@ThreadSafe
public abstract class AbstractMasterClient extends AbstractClient {
  /** Client for determining the master RPC address. */
  private final MasterInquireClient mMasterInquireClient;

  /**
   * Creates a new master client base.
   *
   * @param subject the parent subject
   * @param masterInquireClient the client to use for determining the master rpc address
   */
  public AbstractMasterClient(Subject subject, MasterInquireClient masterInquireClient) {
    super(subject, null);
    mMasterInquireClient = masterInquireClient;
  }

  @Override
  public synchronized InetSocketAddress getAddress() throws UnavailableException {
    return mMasterInquireClient.getPrimaryRpcAddress();
  }
}
