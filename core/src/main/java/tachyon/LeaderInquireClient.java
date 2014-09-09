package tachyon;

import java.util.HashMap;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.data.Stat;

import tachyon.util.CommonUtils;

/**
 * Utility to get leader from zookeeper.
 */
public class LeaderInquireClient {
  private static final int MAX_TRY = 10;
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static HashMap<String, LeaderInquireClient> createdClients =
      new HashMap<String, LeaderInquireClient>();

  public static synchronized LeaderInquireClient getClient(String zookeeperAddress,
      String leaderPath) {
    String key = zookeeperAddress + leaderPath;
    if (!createdClients.containsKey(key)) {
      createdClients.put(key, new LeaderInquireClient(zookeeperAddress, leaderPath));
    }
    return createdClients.get(key);
  }

  private final String mZookeeperAddress;
  private final String mLeaderPath;
  private final CuratorFramework mCLient;

  private LeaderInquireClient(String zookeeperAddress, String leaderPath) {
    mZookeeperAddress = zookeeperAddress;
    mLeaderPath = leaderPath;

    mCLient =
        CuratorFrameworkFactory.newClient(mZookeeperAddress, new ExponentialBackoffRetry(
            Constants.SECOND_MS, 3));
    mCLient.start();
  }

  public synchronized String getMasterAddress() {
    int tried = 0;
    try {
      while (tried < MAX_TRY) {
        if (mCLient.checkExists().forPath(mLeaderPath) != null) {
          List<String> masters = mCLient.getChildren().forPath(mLeaderPath);
          LOG.info("Master addresses: {}", masters);
          if (masters.size() >= 1) {
            if (masters.size() == 1) {
              return masters.get(0);
            }

            long maxTime = 0;
            String leader = "";
            for (String master : masters) {
              Stat stat = mCLient.checkExists().forPath(CommonUtils.concat(mLeaderPath, master));
              if (stat != null && stat.getCtime() > maxTime) {
                maxTime = stat.getCtime();
                leader = master;
              }
            }
            return leader;
          }
        } else {
          LOG.info(mLeaderPath + " does not exist (" + (++tried) + ")");
        }
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }

    return null;
  }
}
