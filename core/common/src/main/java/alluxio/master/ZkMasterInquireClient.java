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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.status.UnavailableException;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.Objects;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility to get leader from zookeeper.
 */
@ThreadSafe
public final class ZkMasterInquireClient implements MasterInquireClient, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ZkMasterInquireClient.class);

  /** Map from key spliced by the address for Zookeeper and path of leader to created client. */
  private static HashMap<String, ZkMasterInquireClient> sCreatedClients = new HashMap<>();

  private final String mZookeeperAddress;
  private final String mElectionPath;
  private final String mLeaderPath;
  private final CuratorFramework mClient;
  private final int mMaxTry;

  /**
   * Gets the client.
   *
   * @param zookeeperAddress the address for Zookeeper
   * @param electionPath the path of the master election
   * @param leaderPath the path of the leader
   *
   * @return the client
   */
  public static synchronized ZkMasterInquireClient getClient(String zookeeperAddress,
      String electionPath, String leaderPath) {
    String key = zookeeperAddress + leaderPath;
    if (!sCreatedClients.containsKey(key)) {
      sCreatedClients.put(key,
          new ZkMasterInquireClient(zookeeperAddress, electionPath, leaderPath));
    }
    return sCreatedClients.get(key);
  }

  /**
   * Constructor for {@link ZkMasterInquireClient}.
   *
   * @param zookeeperAddress the address for Zookeeper
   * @param electionPath the path of the master election
   * @param leaderPath the path of the leader
   */
  private ZkMasterInquireClient(String zookeeperAddress, String electionPath, String leaderPath) {
    mZookeeperAddress = zookeeperAddress;
    mElectionPath = electionPath;
    mLeaderPath = leaderPath;

    LOG.info("Creating new zookeeper client. address: {}", mZookeeperAddress);
    // Start the client lazily.
    mClient = CuratorFrameworkFactory.newClient(mZookeeperAddress,
        new ExponentialBackoffRetry(Constants.SECOND_MS, 3));

    mMaxTry = Configuration.getInt(PropertyKey.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT);
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
      while (tried < mMaxTry) {
        ZooKeeper zookeeper = curatorClient.getZooKeeper();
        if (zookeeper.exists(mLeaderPath, false) != null) {
          List<String> masters = zookeeper.getChildren(mLeaderPath, null);
          LOG.debug("Master addresses: {}", masters);
          if (masters.size() >= 1) {
            if (masters.size() == 1) {
              return NetworkAddressUtils.parseInetSocketAddress(masters.get(0));
            }

            long maxTime = 0;
            String leader = "";
            for (String master : masters) {
              Stat stat = zookeeper.exists(PathUtils.concatPath(mLeaderPath, master), null);
              if (stat != null && stat.getCtime() > maxTime) {
                maxTime = stat.getCtime();
                leader = master;
              }
            }
            LOG.debug("The leader master: {}", leader);
            return NetworkAddressUtils.parseInetSocketAddress(leader);
          }
        } else {
          LOG.info("{} does not exist ({})", mLeaderPath, ++tried);
        }
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOG.error("Error getting the leader master address from zookeeper. Zookeeper address: {}",
          mZookeeperAddress, e);
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
      while (tried < mMaxTry) {
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
      LOG.error("Error getting the master addresses from zookeeper. Zookeeper address: {}",
          mZookeeperAddress, e);
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ZkMasterInquireClient)) {
      return false;
    }
    ZkMasterInquireClient that = (ZkMasterInquireClient) o;
    return mZookeeperAddress.equals(that.mZookeeperAddress)
        && mElectionPath.equals(that.mElectionPath)
        && mLeaderPath.equals(that.mLeaderPath);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mZookeeperAddress, mElectionPath, mLeaderPath);
  }
}
