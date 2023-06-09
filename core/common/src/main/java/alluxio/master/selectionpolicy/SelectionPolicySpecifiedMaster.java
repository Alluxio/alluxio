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

package alluxio.master.selectionpolicy;

import alluxio.exception.status.UnavailableException;
import alluxio.master.MasterInquireClient;

import java.net.InetSocketAddress;

/**
 * The master selection policy that connects to the specified master.
 */
public class SelectionPolicySpecifiedMaster extends AbstractMasterSelectionPolicy {
  final InetSocketAddress mMasterAddressToConnect;

  /**
   * Creates a new selection policy that specifies a master address to client.
   *
   * @param masterAddressToConnect the master address to connect
   */
  protected SelectionPolicySpecifiedMaster(InetSocketAddress masterAddressToConnect) {
    mMasterAddressToConnect = masterAddressToConnect;
  }

  @Override
  public synchronized InetSocketAddress getGrpcMasterAddress(
      MasterInquireClient masterInquireClient) throws UnavailableException {
    return mMasterAddressToConnect;
  }

  @Override
  public Type getType() {
    return Type.SPECIFIED_MASTER;
  }
}

