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

package alluxio.dora.master;

import alluxio.dora.conf.AlluxioConfiguration;
import alluxio.dora.conf.PropertyKey;
import alluxio.dora.exception.status.UnavailableException;
import alluxio.dora.security.user.UserState;
import alluxio.dora.uri.Authority;
import alluxio.dora.util.ConfigurationUtils;
import alluxio.dora.util.network.NetworkAddressUtils;
import alluxio.dora.util.network.NetworkAddressUtils.ServiceType;

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

    @Override
    boolean equals(Object obj);

    @Override
    int hashCode();
  }

  /**
   * Factory for getting a master inquire client.
   */
  class Factory {
    /**
     * @param conf configuration for creating the master inquire client
     * @param userState the user state for the client
     * @return a master inquire client
     */
    public static MasterInquireClient create(AlluxioConfiguration conf, UserState userState) {
      if (conf.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        return ZkMasterInquireClient.getClient(conf.getString(PropertyKey.ZOOKEEPER_ADDRESS),
            conf.getString(PropertyKey.ZOOKEEPER_ELECTION_PATH),
            conf.getString(PropertyKey.ZOOKEEPER_LEADER_PATH),
            conf.getInt(PropertyKey.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT),
            conf.getBoolean(PropertyKey.ZOOKEEPER_AUTH_ENABLED));
      } else {
        List<InetSocketAddress> addresses = ConfigurationUtils.getMasterRpcAddresses(conf);
        if (addresses.size() > 1) {
          return new PollingMasterInquireClient(addresses, conf, userState,
              alluxio.grpc.ServiceType.META_MASTER_CLIENT_SERVICE);
        } else {
          return new SingleMasterInquireClient(addresses.get(0));
        }
      }
    }

    /**
     * @param addresses the addresses to use for the client
     * @param conf configuration for creating the master inquire client
     * @param userState the user state for the client
     * @return a master inquire client using the addresses given
     */
    public static MasterInquireClient createForAddresses(
        List<InetSocketAddress> addresses, AlluxioConfiguration conf, UserState userState) {
      if (addresses.size() > 1) {
        return new PollingMasterInquireClient(addresses, conf, userState,
            alluxio.grpc.ServiceType.META_MASTER_CLIENT_SERVICE);
      } else {
        return new SingleMasterInquireClient(addresses.get(0));
      }
    }

    /**
     * @param conf configuration for creating the master inquire client
     * @param userState the user state for the client
     * @return a master inquire client
     */
    public static MasterInquireClient createForJobMaster(AlluxioConfiguration conf,
        UserState userState) {
      if (conf.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        return ZkMasterInquireClient.getClient(conf.getString(PropertyKey.ZOOKEEPER_ADDRESS),
            conf.getString(PropertyKey.ZOOKEEPER_JOB_ELECTION_PATH),
            conf.getString(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH),
            conf.getInt(PropertyKey.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT),
            conf.getBoolean(PropertyKey.ZOOKEEPER_AUTH_ENABLED));
      } else {
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(conf);
        if (addresses.size() > 1) {
          return new PollingMasterInquireClient(addresses, conf, userState,
              alluxio.grpc.ServiceType.JOB_MASTER_CLIENT_SERVICE);
        } else {
          return new SingleMasterInquireClient(addresses.get(0));
        }
      }
    }

    /**
     * @param conf configuration for creating the master inquire client
     * @return the connect string represented by the configuration
     */
    public static ConnectDetails getConnectDetails(AlluxioConfiguration conf) {
      if (conf.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        return new ZkMasterInquireClient.ZkMasterConnectDetails(conf.getString(PropertyKey.ZOOKEEPER_ADDRESS),
            conf.getString(PropertyKey.ZOOKEEPER_LEADER_PATH));
      } else if (ConfigurationUtils.getMasterRpcAddresses(conf).size() > 1) {
        return new PollingMasterInquireClient.MultiMasterConnectDetails(
            ConfigurationUtils.getMasterRpcAddresses(conf));
      } else {
        return new SingleMasterInquireClient.SingleMasterConnectDetails(
            NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, conf));
      }
    }

    private Factory() {
    } // Not intended for instantiation.
  }
}
