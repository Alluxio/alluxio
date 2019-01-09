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

import alluxio.AlluxioConfiguration;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.status.UnavailableException;
import alluxio.master.SingleMasterInquireClient.SingleMasterConnectDetails;
import alluxio.master.ZkMasterInquireClient.ZkMasterConnectDetails;
import alluxio.uri.Authority;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import java.net.InetSocketAddress;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Client for determining the primary master.
 */
@ThreadSafe
public interface MasterInquireClient {
  /**
   * @return the rpc address of the primary master. The implementation should perform retries if
   *         appropriate
   * @throws UnavailableException if the primary rpc address cannot be determined
   */
  InetSocketAddress getPrimaryRpcAddress() throws UnavailableException;

  /**
   * @return a list of all masters' RPC addresses
   * @throws UnavailableException if the master rpc addresses cannot be determined
   */
  List<InetSocketAddress> getMasterRpcAddresses() throws UnavailableException;

  /**
   * Returns canonical connect details representing how this client connects to the master.
   *
   * @return the connect details
   */
  ConnectDetails getConnectDetails();

  /**
   * Interface for representing master inquire connect details.
   *
   * Connect info should be unique so that if two inquire clients have the same connect info, they
   * connect to the same cluster.
   */
  interface ConnectDetails {
    /**
     * @return an authority string representing the connect details
     */
    Authority toAuthority();
  }

  /**
   * Factory for getting a master inquire client.
   */
  class Factory {
    /**
     * Creates an instance of {@link MasterInquireClient} based on the current configuration. The
     * returned instance may be shared, so it should not be closed by callers of this method.
     *
     * @return a master inquire client
     */
    public static MasterInquireClient create() {
      return create(Configuration.global());
    }

    /**
     * @param conf configuration for creating the master inquire client
     * @return a master inquire client
     */
    public static MasterInquireClient create(AlluxioConfiguration conf) {
      if (conf.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        return ZkMasterInquireClient.getClient(conf.get(PropertyKey.ZOOKEEPER_ADDRESS),
            conf.get(PropertyKey.ZOOKEEPER_ELECTION_PATH),
            conf.get(PropertyKey.ZOOKEEPER_LEADER_PATH));
      } else if (ConfigurationUtils.getMasterRpcAddresses(conf).size() > 1) {
        return new PollingMasterInquireClient(
            ConfigurationUtils.getMasterRpcAddresses(conf));
      } else {
        return new SingleMasterInquireClient(
            NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf));
      }
    }

    public static MasterInquireClient createForJobMaster() {
      if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        return ZkMasterInquireClient.getClient(Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS),
            Configuration.get(PropertyKey.ZOOKEEPER_JOB_ELECTION_PATH),
            Configuration.get(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH));
      } else if (ConfigurationUtils.getJobMasterRpcAddresses(Configuration.global())
          .size() > 1) {
        return new PollingMasterInquireClient(
            ConfigurationUtils.getJobMasterRpcAddresses(Configuration.global()));
      } else {
        return new SingleMasterInquireClient(
            NetworkAddressUtils.getConnectAddress(ServiceType.JOB_MASTER_RPC));
      }
    }
    /**
     * @param conf configuration for creating the master inquire client
     * @return the connect string represented by the configuration
     */
    public static ConnectDetails getConnectDetails(AlluxioConfiguration conf) {
      if (conf.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        return new ZkMasterConnectDetails(conf.get(PropertyKey.ZOOKEEPER_ADDRESS),
            conf.get(PropertyKey.ZOOKEEPER_LEADER_PATH));
      } else {
        return new SingleMasterConnectDetails(
            NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf));
      }
    }

    private Factory() {
    } // Not intended for instantiation.
  }
}
