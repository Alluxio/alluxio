package tachyon;

import java.util.List;
import java.util.HashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

/**
 * Utility to get leader from zookeeper.
 */
public class LeaderInquireClient {
  private final static int MAX_TRY = 10;

  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final String ZOOKEEPER_ADDRESS;
  private final String LEADER_PATH;
  private final CuratorFramework CLIENT;

  private static HashMap<String, LeaderInquireClient> createdClients =
      new HashMap<String, LeaderInquireClient>();

  private LeaderInquireClient(String zookeeperAddress, String leaderPath) {
    ZOOKEEPER_ADDRESS = zookeeperAddress;
    LEADER_PATH = leaderPath;

    CLIENT = CuratorFrameworkFactory.newClient(
        ZOOKEEPER_ADDRESS, new ExponentialBackoffRetry(1000, 3));
    CLIENT.start();
  }

  public static synchronized LeaderInquireClient
      getClient(String zookeeperAddress, String leaderPath) {
    String key = zookeeperAddress + leaderPath;
    if (!createdClients.containsKey(key)) {
      createdClients.put(key, new LeaderInquireClient(zookeeperAddress, leaderPath));
    }
    return createdClients.get(key);
  }

  public synchronized String getMasterAddress() {
    int tried = 0;
    try {
      while (tried < MAX_TRY) {
        if (CLIENT.checkExists().forPath(LEADER_PATH) != null) {
          List<String> masters = CLIENT.getChildren().forPath(LEADER_PATH);
          LOG.info(masters);
          if (masters.size() >= 1) {
            if (masters.size() == 1) {
              return masters.get(0);
            }

            long maxTime = 0;
            String leader = "";
            for (String master: masters) {
              Stat stat = CLIENT.checkExists().forPath(LEADER_PATH + "/" + master);
              if (stat != null && stat.getCtime() > maxTime) {
                maxTime = stat.getCtime();
                leader = master;
              }
            }
            return leader;
          }
        } else {
          LOG.info(LEADER_PATH + " does not exist (" + (++ tried) + ")");
        }
        CommonUtils.sleepMs(LOG, 1000);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }

    return null;
  }
}