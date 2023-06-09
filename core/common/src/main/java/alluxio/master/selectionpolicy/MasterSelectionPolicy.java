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
 * Interface for master selection policy that
 * determines which master node a client should connect to.
 */
public interface MasterSelectionPolicy {
  /**
   * The enum for master selection policies.
   */
  enum Type {
    PRIMARY_MASTER,
    ANY_STANDBY_MASTER,
    ANY_MASTER,
    SPECIFIED_MASTER,
  }

  /**
   * Get and cache the primary master address.
   *
   * @param masterInquireClient master inquire client
   *
   * @return the remote address of primary master gRPC server
   * @throws UnavailableException if address cannot be determined
   */
  InetSocketAddress getPrimaryMasterAddressCached(MasterInquireClient masterInquireClient)
      throws UnavailableException;

  /**
   * Gets the master address the client makes gRPC request to.
   *
   * @param masterInquireClient master inquire client
   *
   * @return the remote address of master gRPC server
   * @throws UnavailableException if address cannot be determined
   */
  InetSocketAddress getGrpcMasterAddress(MasterInquireClient masterInquireClient)
      throws UnavailableException;

  /**
   * Resets the cached primary master address.
   */
  void resetPrimaryMasterAddressCache();

  /**
   * @return the type of the master selection policy
   */
  Type getType();

  /**
   * Factory for {@link MasterSelectionPolicy}.
   */
  class Factory {
    /**
     * Creates a MasterSelectionPolicy that selects the primary master to connect.
     * @return the MasterSelectionPolicy
     */
    public static MasterSelectionPolicy primaryMaster() {
      return new SelectionPolicyPrimaryMaster();
    }

    /**
     * Creates a MasterSelectionPolicy that selects a standby master to connect.
     * @return the MasterSelectionPolicy
     */
    public static MasterSelectionPolicy anyStandbyMaster() {
      return new SelectionPolicyAnyStandbyMaster();
    }

    /**
     * Creates a MasterSelectionPolicy that selects a standby master to connect.
     * @param randomSeed a random seed to do the deterministic random selection
     * @return the MasterSelectionPolicy
     */
    public static MasterSelectionPolicy anyStandbyMaster(long randomSeed) {
      return new SelectionPolicyAnyStandbyMaster(randomSeed);
    }

    /**
     * Creates a MasterSelectionPolicy that selects a random master to connect.
     * @return the MasterSelectionPolicy
     */
    public static MasterSelectionPolicy anyMaster() {
      return new SelectionPolicyAnyMaster();
    }

    /**
     * Creates a MasterSelectionPolicy that selects a specified master to connect.
     * @param masterAddress the master address to connect
     * @return the MasterSelectionPolicy
     */
    public static MasterSelectionPolicy specifiedMaster(InetSocketAddress masterAddress) {
      return new SelectionPolicySpecifiedMaster(masterAddress);
    }
  }
}
