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

package alluxio;

import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility to get leader from zookeeper.
 */
@ThreadSafe
public final class MasterInquireClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Map from key spliced by the address for ZooKeeper and path of leader to created client. */
  private static HashMap<String, MasterInquireClient> sCreatedClients = new HashMap<>();

  /**
   * Gets the client.
   *
   * @param zookeeperAddress the address for ZooKeeper
   * @param electionPath the path of the master election
   * @param leaderPath the path of the leader
   *
   * @return the client
   */
  public static synchronized MasterInquireClient getClient(String zookeeperAddress,
      String electionPath, String leaderPath) {
    String key = zookeeperAddress + leaderPath;
    if (!sCreatedClients.containsKey(key)) {
      sCreatedClients.put(key, new MasterInquireClient(zookeeperAddress, electionPath, leaderPath));
    }
    return sCreatedClients.get(key);
  }

  private final String mZooKeeperAddress;
  private final String mElectionPath;
  private final String mLeaderPath;
  private final CuratorFramework mClient;
  private final int mMaxTry;

  private MasterInquireClient(String zookeeperAddress, String electionPath, String leaderPath) {
    mZooKeeperAddress = zookeeperAddress;
    mElectionPath = electionPath;
    mLeaderPath = leaderPath;

    LOG.info("Creating new zookeeper client. address: {}", mZooKeeperAddress);
    mClient =
        CuratorFrameworkFactory.newClient(mZooKeeperAddress, new ExponentialBackoffRetry(
            Constants.SECOND_MS, 3));
    mClient.start();

    mMaxTry = Configuration.getInt(PropertyKey.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT);
  }

  /**
   * @return the ZooKeeperNode for the current leader master
   */
  public synchronized MasterZooKeeperNode getLeader() {
    int tried = 0;
    try {
      while (tried < mMaxTry) {
        if (mClient.checkExists().forPath(mLeaderPath) != null) {
          List<String> masters = mClient.getChildren().forPath(mLeaderPath);
          LOG.info("Master addresses: {}", masters);
          if (masters.size() >= 1) {
            if (masters.size() == 1) {
              return MasterZooKeeperNode.deserialize(masters.get(0));
            }

            long maxTime = 0;
            String leader = "";
            for (String master : masters) {
              Stat stat = mClient.checkExists().forPath(
                  PathUtils.concatPath(mLeaderPath, master));
              if (stat != null && stat.getCtime() > maxTime) {
                maxTime = stat.getCtime();
                leader = master;
              }
            }
            LOG.info("The leader master: {}", leader);
            return MasterZooKeeperNode.deserialize(leader);
          }
        } else {
          LOG.info("{} does not exist ({})", mLeaderPath, ++tried);
        }
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
      }
    } catch (Exception e) {
      LOG.error("Error getting the leader master address from zookeeper. ZooKeeper address: {}",
          mZooKeeperAddress, e);
    }

    return null;
  }

  /**
   * @return the list of all the master addresses
   */
  public synchronized List<String> getMasterAddresses() {
    int tried = 0;
    try {
      while (tried < mMaxTry) {
        if (mClient.checkExists().forPath(mElectionPath) != null) {
          List<String> children = mClient.getChildren().forPath(mElectionPath);
          List<String> ret = new ArrayList<>();
          for (String child : children) {
            byte[] data = mClient.getData().forPath(PathUtils.concatPath(mElectionPath, child));
            if (data != null) {
              // The data of the child znode is the corresponding master address for now
              ret.add(new String(data, "utf-8"));
            }
          }
          LOG.info("All masters: {}", ret);
          return ret;
        } else {
          LOG.info("{} does not exist ({})", mElectionPath, ++tried);
        }
      }
    } catch (Exception e) {
      LOG.error("Error getting the master addresses from zookeeper. ZooKeeper address: {}",
          mZooKeeperAddress, e);
    }

    return null;
  }
}
