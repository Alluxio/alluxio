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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import javax.annotation.Nullable;

/**
 * The master connection mode that connects to a random standby master.
 */
public class SelectionPolicyAnyStandbyMaster extends AbstractMasterSelectionPolicy {
  @Nullable
  private final Random mRandom;

  /**
   * Creates a new standby connection mode that picks the standby master randomly.
   */
  protected SelectionPolicyAnyStandbyMaster() {
    mRandom = null;
  }

  /**
   * Creates a new standby connection mode that
   * picks the standby master deterministically with a random seed.
   *
   * @param shuffleSeed the random seed to shuffle the master client list
   */
  protected SelectionPolicyAnyStandbyMaster(long shuffleSeed) {
    mRandom = new Random(shuffleSeed);
  }

  @Override
  public synchronized InetSocketAddress getGrpcMasterAddress(
      MasterInquireClient masterInquireClient) throws UnavailableException {
    List<InetSocketAddress> masterAddresses =
        new ArrayList<>(masterInquireClient.getMasterRpcAddresses());

    if (mRandom != null) {
      masterAddresses.sort(Comparator.comparing(InetSocketAddress::toString));
      Collections.shuffle(masterAddresses, mRandom);
    } else {
      Collections.shuffle(masterAddresses);
    }
    for (InetSocketAddress address : masterAddresses) {
      if (!address.equals(getPrimaryMasterAddressCached(masterInquireClient))) {
        return address;
      }
    }
    throw new UnavailableException("No standby masters available");
  }

  @Override
  public Type getType() {
    return Type.ANY_STANDBY_MASTER;
  }
}
