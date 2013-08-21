package tachyon;

import java.util.List;

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

  public LeaderInquireClient(String zookeeperAddress, String leaderPath) {
    ZOOKEEPER_ADDRESS = zookeeperAddress;
    LEADER_PATH = leaderPath;

    System.out.println(ZOOKEEPER_ADDRESS + " " + LEADER_PATH);
    CLIENT = CuratorFrameworkFactory.newClient(
        ZOOKEEPER_ADDRESS, new ExponentialBackoffRetry(1000, 3));
    CLIENT.start();
  }

  public synchronized String getMasterAddress() {
    int tried = 0;
    try {
      while (tried < MAX_TRY) {
        System.out.println("LIC 1");
        if (CLIENT.checkExists().forPath(LEADER_PATH) != null) {
          System.out.println("LIC 2");
          List<String> masters = CLIENT.getChildren().forPath(LEADER_PATH);
          LOG.info(masters);
          System.out.println("LIC 3" + masters);
          if (masters.size() >= 1) {
            if (masters.size() == 1) {
              return masters.get(0);
            }

            long maxTime = 0;
            String leader = "";
            for (String master: masters) {
              Stat stat = CLIENT.checkExists().forPath(LEADER_PATH + master);
              if (stat != null && stat.getCtime() > maxTime) {
                maxTime = stat.getCtime();
                leader = master;
              }
            }
            return leader;
          }
        } else {
          LOG.info(LEADER_PATH + " does not exist (" + (++ tried) + ")");
          System.out.println(LEADER_PATH + " does not exist (" + (++ tried) + ")");
        }
        CommonUtils.sleepMs(LOG, 1000);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }

    return null;
  }
}
