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

import alluxio.Constants;
import alluxio.exception.status.UnavailableException;
import alluxio.uri.Authority;
import alluxio.uri.ZookeeperAuthority;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility to get leader from zookeeper.
 */
@ThreadSafe
public final class ZkMasterInquireClient implements MasterInquireClient, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ZkMasterInquireClient.class);

  /** Map from key spliced by the address for Zookeeper and path of leader to created client. */
  private static HashMap<ZkMasterConnectDetails, ZkMasterInquireClient> sCreatedClients =
      new HashMap<>();

  private final ZkMasterConnectDetails mConnectDetails;
  private final String mElectionPath;
  private final CuratorFramework mClient;
  private final int mInquireRetryCount;

  /**
   * Zookeeper factory for curator that controls enabling/disabling client authentication.
   */
  private class AlluxioZookeeperFactory implements ZookeeperFactory {
    private boolean mAuthEnabled;

    public AlluxioZookeeperFactory(boolean authEnabled) {
      mAuthEnabled = authEnabled;
    }

    @Override
    public ZooKeeper newZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
                                  boolean canBeReadOnly) throws Exception {
      ZKClientConfig zkConfig = new ZKClientConfig();
      zkConfig.setProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY,
          Boolean.toString(mAuthEnabled).toLowerCase());
      return new ZooKeeper(connectString, sessionTimeout, watcher, zkConfig);
    }
  }

  /**
   * Gets the client.
   *
   * @param zookeeperAddress the address for Zookeeper
   * @param electionPath the path of the master election
   * @param leaderPath the path of the leader
   * @param inquireRetryCount the number of times to retry connections
   * @param authEnabled if Alluxio client-side auth is enabled
   * @return the client
   */
  public static synchronized ZkMasterInquireClient getClient(String zookeeperAddress,
      String electionPath, String leaderPath, int inquireRetryCount, boolean authEnabled) {
    ZkMasterConnectDetails connectDetails =
        new ZkMasterConnectDetails(zookeeperAddress, leaderPath);
    if (!sCreatedClients.containsKey(connectDetails)) {
      sCreatedClients.put(connectDetails,
          new ZkMasterInquireClient(connectDetails, electionPath, inquireRetryCount, authEnabled));
    }
    return sCreatedClients.get(connectDetails);
  }

  /**
   * Constructor for {@link ZkMasterInquireClient}.
   *
   * @param connectDetails connect details
   * @param electionPath the path of the master election
   * @param inquireRetryCount the number of times to retry connections
   * @param authEnabled if Alluxio client-side auth is enabled
   */
  private ZkMasterInquireClient(ZkMasterConnectDetails connectDetails, String electionPath,
      int inquireRetryCount, boolean authEnabled) {
    mConnectDetails = connectDetails;
    mElectionPath = electionPath;

    LOG.info("Creating new zookeeper client for {}", connectDetails);
    CuratorFrameworkFactory.Builder curatorBuilder = CuratorFrameworkFactory.builder();
    curatorBuilder.connectString(connectDetails.getZkAddress());
    curatorBuilder.retryPolicy(new ExponentialBackoffRetry(Constants.SECOND_MS, 3));
    curatorBuilder.zookeeperFactory(new AlluxioZookeeperFactory(authEnabled));
    mClient = curatorBuilder.build();

    mInquireRetryCount = inquireRetryCount;
  }

  @Override
  public synchronized InetSocketAddress getPrimaryRpcAddress() throws UnavailableException {
    ensureStarted();
    long startTime = System.currentTimeMillis();
    int tried = 0;
    try {
      CuratorZookeeperClient curatorClient = mClient.getZookeeperClient();
      // #blockUntilConnectedOrTimedOut() will block for at least 1 second, even if the client is
      // connected within a few milliseconds. We improve the latency here by first waiting on the
      // connection status explicitly.
      for (int i = 0; i < 50; i++) {
        if (curatorClient.isConnected()) {
          break;
        }
        CommonUtils.sleepMs(20);
      }
      curatorClient.blockUntilConnectedOrTimedOut();
      String leaderPath = mConnectDetails.getLeaderPath();
      while (tried++ < mInquireRetryCount) {
        ZooKeeper zookeeper = curatorClient.getZooKeeper();
        if (zookeeper.exists(leaderPath, false) != null) {
          List<String> masters = zookeeper.getChildren(leaderPath, null);
          LOG.debug("Master addresses: {}", masters);
          if (masters.size() >= 1) {
            if (masters.size() == 1) {
              return NetworkAddressUtils.parseInetSocketAddress(masters.get(0));
            }

            long maxTime = 0;
            String leader = "";
            for (String master : masters) {
              Stat stat = zookeeper.exists(PathUtils.concatPath(leaderPath, master), null);
              if (stat != null && stat.getCtime() > maxTime) {
                maxTime = stat.getCtime();
                leader = master;
              }
            }
            LOG.debug("The leader master: {}", leader);
            return NetworkAddressUtils.parseInetSocketAddress(leader);
          }
        } else {
          LOG.info("{} does not exist ({})", leaderPath, tried);
        }
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOG.error("Error getting the leader master address from zookeeper. Zookeeper: {}",
          mConnectDetails, e);
    } finally {
      LOG.debug("Finished getPrimaryRpcAddress() in {}ms", System.currentTimeMillis() - startTime);
    }

    throw new UnavailableException("Failed to determine primary master rpc address");
  }

  @Override
  public synchronized List<InetSocketAddress> getMasterRpcAddresses() throws UnavailableException {
    ensureStarted();
    int tried = 0;
    try {
      while (tried < mInquireRetryCount) {
        if (mClient.checkExists().forPath(mElectionPath) != null) {
          List<String> children = mClient.getChildren().forPath(mElectionPath);
          List<InetSocketAddress> ret = new ArrayList<>();
          for (String child : children) {
            byte[] data = mClient.getData().forPath(PathUtils.concatPath(mElectionPath, child));
            if (data != null) {
              // The data of the child znode is the corresponding master address for now
              ret.add(NetworkAddressUtils.parseInetSocketAddress(new String(data, "utf-8")));
            }
          }
          LOG.info("All masters: {}", ret);
          return ret;
        } else {
          LOG.info("{} does not exist ({})", mElectionPath, ++tried);
        }
      }
    } catch (Exception e) {
      LOG.error("Error getting the master addresses from zookeeper. Zookeeper: {}", mConnectDetails,
          e);
    }

    throw new UnavailableException("Failed to query zookeeper for master RPC addresses");
  }

  private synchronized void ensureStarted() {
    switch (mClient.getState()) {
      case LATENT:
        mClient.start();
        return;
      case STARTED:
        return;
      case STOPPED:
        throw new IllegalStateException("Client has already been closed");
      default:
        throw new IllegalStateException("Unknown state: " + mClient.getState());
    }
  }

  @Override
  public void close() {
    mClient.close();
  }

  @Override
  public ConnectDetails getConnectDetails() {
    return mConnectDetails;
  }

  /**
   * Details used to connect to the leader Alluxio master via Zookeeper.
   */
  public static class ZkMasterConnectDetails implements ConnectDetails {
    private final String mZkAddress;
    private final String mLeaderPath;

    /**
     * @param zkAddress a zookeeper address
     * @param leaderPath a leader path
     */
    public ZkMasterConnectDetails(String zkAddress, String leaderPath) {
      mZkAddress = zkAddress;
      mLeaderPath = leaderPath;
    }

    /**
     * @return the zookeeper address
     */
    public String getZkAddress() {
      return mZkAddress;
    }

    /**
     * @return the leader path
     */
    public String getLeaderPath() {
      return mLeaderPath;
    }

    @Override
    public Authority toAuthority() {
      return new ZookeeperAuthority(mZkAddress);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ZkMasterConnectDetails)) {
        return false;
      }
      ZkMasterConnectDetails that = (ZkMasterConnectDetails) o;
      return mZkAddress.equals(that.mZkAddress)
          && mLeaderPath.equals(that.mLeaderPath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mZkAddress, mLeaderPath);
    }

    @Override
    public String toString() {
      return toAuthority() + mLeaderPath;
    }
  }
}
