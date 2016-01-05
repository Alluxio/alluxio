/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon;

import java.util.HashMap;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;
import tachyon.util.io.PathUtils;

/**
 * Utility to get leader from zookeeper.
 */
public final class LeaderInquireClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static HashMap<String, LeaderInquireClient> sCreatedClients =
      new HashMap<String, LeaderInquireClient>();

  /**
   * Gets the client.
   *
   * @param zookeeperAddress the address for Zookeeper
   * @param leaderPath the path of the leader
   * @param tachyonConf the configuration for Tachyon
   *
   * @return the client
   */
  public static synchronized LeaderInquireClient getClient(String zookeeperAddress,
      String leaderPath, TachyonConf tachyonConf) {
    String key = zookeeperAddress + leaderPath;
    if (!sCreatedClients.containsKey(key)) {
      sCreatedClients.put(key, new LeaderInquireClient(zookeeperAddress, leaderPath, tachyonConf));
    }
    return sCreatedClients.get(key);
  }

  private final String mZookeeperAddress;
  private final String mLeaderPath;
  private final CuratorFramework mClient;
  private final int mMaxTry;

  private LeaderInquireClient(String zookeeperAddress, String leaderPath, TachyonConf tachyonConf) {
    mZookeeperAddress = zookeeperAddress;
    mLeaderPath = leaderPath;

    LOG.info("create new zookeeper client. address: {}", mZookeeperAddress);
    mClient =
        CuratorFrameworkFactory.newClient(mZookeeperAddress, new ExponentialBackoffRetry(
            Constants.SECOND_MS, 3));
    mClient.start();

    mMaxTry = tachyonConf.getInt(Constants.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT);
  }

  /**
   * Gets the address of the master.
   *
   * @return the address of the master
   */
  public synchronized String getMasterAddress() {
    int tried = 0;
    try {
      while (tried < mMaxTry) {
        if (mClient.checkExists().forPath(mLeaderPath) != null) {
          List<String> masters = mClient.getChildren().forPath(mLeaderPath);
          LOG.info("Master addresses: {}", masters);
          if (masters.size() >= 1) {
            if (masters.size() == 1) {
              return masters.get(0);
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
            return leader;
          }
        } else {
          LOG.info("{} does not exist ({})", mLeaderPath, (++tried));
        }
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
      }
    } catch (Exception e) {
      LOG.error("Error with zookeeper: {} message: {}", mZookeeperAddress, e.getMessage());
    }

    return null;
  }
}
