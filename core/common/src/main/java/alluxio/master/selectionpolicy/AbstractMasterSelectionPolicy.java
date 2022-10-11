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
import javax.annotation.Nullable;

/**
 * Base class of master selection policy that
 * determines which master node a client should connect to.
 */
public abstract class AbstractMasterSelectionPolicy implements MasterSelectionPolicy {
  @Nullable
  protected volatile InetSocketAddress mPrimaryMasterAddress;

  @Override
  public synchronized InetSocketAddress getPrimaryMasterAddressCached(
      MasterInquireClient masterInquireClient) throws UnavailableException {
    if (mPrimaryMasterAddress == null) {
      mPrimaryMasterAddress = masterInquireClient.getPrimaryRpcAddress();
    }
    return mPrimaryMasterAddress;
  }

  @Override
  public abstract InetSocketAddress getGrpcMasterAddress(MasterInquireClient masterInquireClient)
      throws UnavailableException;

  @Override
  public synchronized void resetPrimaryMasterAddressCache() {
    mPrimaryMasterAddress = null;
  }
}
