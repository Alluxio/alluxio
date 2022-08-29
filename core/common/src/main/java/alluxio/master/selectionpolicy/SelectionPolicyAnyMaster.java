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
import java.util.List;

/**
 * The master selection policy that connects to a random master.
 */
public class SelectionPolicyAnyMaster extends AbstractMasterSelectionPolicy {
  protected SelectionPolicyAnyMaster() {}

  @Override
  public synchronized InetSocketAddress getGrpcMasterAddress(
      MasterInquireClient masterInquireClient) throws UnavailableException {
    List<InetSocketAddress> masterAddresses =
        new ArrayList<>(masterInquireClient.getMasterRpcAddresses());
    if (masterAddresses.size() == 0) {
      throw new UnavailableException("No master available");
    }
    Collections.shuffle(masterAddresses);
    return masterAddresses.get(0);
  }
}
