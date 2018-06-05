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

import static java.util.stream.Collectors.joining;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.status.UnavailableException;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;

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
   * Returns a canonical connect string representing how this client connects to the master.
   *
   * @return the connect string
   */
  ConnectString getConnectString();

  /**
   * Class for representing master inquire connect strings.
   *
   * Canonical connect strings are unique so that if two inquire clients have the same
   * connect string, they connect to the same cluster.
   *
   * Examples: "zk://host1:port1,host2:port2/leader/path", "master:19998"
   */
  class ConnectString {
    private final String mString;

    private ConnectString(String string) {
      mString = string;
    }

    /**
     * @param zkAddress zookeeper address, e.g. "zkhost1:port1,zkhost2:port2"
     * @param leaderPath zookeeper leader path
     * @return a zookeeper connect string
     */
    public static ConnectString zkConnectString(String zkAddress, String leaderPath) {
      return new ConnectString("zk://" + zkAddress + leaderPath);
    }

    /**
     * @param addr master address
     * @return a single master connect string
     */
    public static ConnectString singleMasterConnectString(InetSocketAddress addr) {
      return new ConnectString(format(addr));
    }

    /**
     * @param masterAddresses master addresses
     * @return a multi master connect string
     */
    public static ConnectString multiMasterConnectString(List<InetSocketAddress> masterAddresses) {
      return new ConnectString(
          masterAddresses.stream().map(ConnectString::format).collect(joining(",")));
    }

    private static String format(InetSocketAddress addr) {
      return addr.getHostString() + ":" + addr.getPort();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ConnectString)) {
        return false;
      }
      ConnectString that = (ConnectString) o;
      return mString.equals(that.mString);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mString);
    }

    @Override
    public String toString() {
      return mString;
    }
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
      return create(Config.defaults());
    }

    /**
     * @param config configuration for creating the master inquire client
     * @return a master inquire client
     */
    public static MasterInquireClient create(Config config) {
      if (config.isZookeeperEnabled()) {
        return ZkMasterInquireClient.getClient(config.getZookeeperAddress(),
            config.getElectionPath(), config.getLeaderPath());
      } else {
        return new SingleMasterInquireClient(
            new InetSocketAddress(config.getConnectHost(), config.getConnectPort()));
      }
    }

    /**
     * @param config inquire client configuration
     * @return the connect string represented by the configuration
     */
    public static ConnectString getConnectString(Config config) {
      if (config.isZookeeperEnabled()) {
        return ConnectString.zkConnectString(config.getZookeeperAddress(),
            config.getLeaderPath());
      } else {
        return ConnectString.singleMasterConnectString(
            new InetSocketAddress(config.getConnectHost(), config.getConnectPort()));
      }
    }

    private Factory() {} // Not intended for instantiation.

    /**
     * Configuration for building a {@link MasterInquireClient} from a
     * {@link MasterInquireClient.Factory}.
     */
    public static final class Config {
      // HA connect with Zookeeper.
      private boolean mZookeeperEnabled;
      private String mZookeeperAddress;
      private String mElectionPath;
      private String mLeaderPath;

      // Non-HA connect.
      private String mConnectHost;
      private int mConnectPort;

      // Use Config.defaults() instead.
      private Config() {}

      /**
       * @return the default master inquire configuration based on {@link Configuration}
       */
      public static Config defaults() {
        String zkAddress = Configuration.containsKey(PropertyKey.ZOOKEEPER_ADDRESS)
            ? Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS)
            : null;
        return new Config()
            .setZookeeperEnabled(Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED))
            .setZookeeperAddress(zkAddress)
            .setElectionPath(Configuration.get(PropertyKey.ZOOKEEPER_ELECTION_PATH))
            .setLeaderPath(Configuration.get(PropertyKey.ZOOKEEPER_LEADER_PATH))
            .setConnectHost(
                NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC).getHostName())
            .setConnectPort(
                NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC).getPort());
      }

      /**
       * @param zookeeperEnabled whether zookeeper is enabled
       * @return this
       */
      public Config setZookeeperEnabled(boolean zookeeperEnabled) {
        mZookeeperEnabled = zookeeperEnabled;
        return this;
      }

      /**
       * @param zookeeperAddress zookeeper address
       * @return this
       */
      public Config setZookeeperAddress(String zookeeperAddress) {
        mZookeeperAddress = zookeeperAddress;
        return this;
      }

      /**
       * @param electionPath election path
       * @return this
       */
      public Config setElectionPath(String electionPath) {
        mElectionPath = electionPath;
        return this;
      }

      /**
       * @param leaderPath leader path
       * @return this
       */
      public Config setLeaderPath(String leaderPath) {
        mLeaderPath = leaderPath;
        return this;
      }

      /**
       * @param host master connect host
       * @return this
       */
      public Config setConnectHost(String host) {
        mConnectHost = host;
        return this;
      }

      /**
       * @param port master connect port
       * @return this
       */
      public Config setConnectPort(int port) {
        mConnectPort = port;
        return this;
      }

      /**
       * @return whether zookeeper is enabled
       */
      public boolean isZookeeperEnabled() {
        return mZookeeperEnabled;
      }

      /**
       * @return the zookeeper address
       */
      public String getZookeeperAddress() {
        return mZookeeperAddress;
      }

      /**
       * @return the election path
       */
      public String getElectionPath() {
        return mElectionPath;
      }

      /**
       * @return the leader path
       */
      public String getLeaderPath() {
        return mLeaderPath;
      }

      /**
       * @return the connect host
       */
      public String getConnectHost() {
        return mConnectHost;
      }

      /**
       * @return the connect port
       */
      public int getConnectPort() {
        return mConnectPort;
      }
    }
  }
}
