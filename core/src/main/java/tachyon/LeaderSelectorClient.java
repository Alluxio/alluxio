package tachyon;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

/**
 * Masters use this client to elect a leader.
 */
public class LeaderSelectorClient implements Closeable, LeaderSelectorListener {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final String mZookeeperAddress;
  private final String mElectionPath;
  private final String mLeaderFolder;
  private final String mName;
  private final LeaderSelector mLeaderSelector;

  private AtomicBoolean mIsLeader = new AtomicBoolean(false);
  private volatile Thread mCurrentMasterThread = null;

  public LeaderSelectorClient(String zookeeperAddress, String electionPath, String leaderPath,
      String name) {
    mZookeeperAddress = zookeeperAddress;
    mElectionPath = electionPath;
    if (leaderPath.endsWith(Constants.PATH_SEPARATOR)) {
      mLeaderFolder = leaderPath;
    } else {
      mLeaderFolder = leaderPath + Constants.PATH_SEPARATOR;
    }
    mName = name;

    // Create a leader selector using the given path for management.
    // All participants in a given leader selection must use the same path.
    // ExampleClient here is also a LeaderSelectorListener but this isn't required.
    CuratorFramework client =
        CuratorFrameworkFactory.newClient(mZookeeperAddress, new ExponentialBackoffRetry(
            Constants.SECOND_MS, 3));
    client.start();
    mLeaderSelector = new LeaderSelector(client, mElectionPath, this);
    mLeaderSelector.setId(name);

    // for most cases you will want your instance to requeue when it relinquishes leadership
    mLeaderSelector.autoRequeue();
  }

  @Override
  public void close() throws IOException {
    if (mCurrentMasterThread != null) {
      mCurrentMasterThread.interrupt();
    }

    try {
      mLeaderSelector.close();
    } catch (IllegalStateException e) {
      // TODO This should not happen in unit tests.
      if (!e.getMessage().equals("Already closed or has not been started")) {
        throw e;
      }
    }
  }

  public String getName() {
    return mName;
  }

  public List<String> getParticipants() {
    try {
      List<Participant> participants =
          new ArrayList<Participant>(mLeaderSelector.getParticipants());
      List<String> results = new ArrayList<String>();
      for (Participant part : participants) {
        results.add(part.getId());
      }
      return results;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  public boolean isLeader() {
    return mIsLeader.get();
  }

  /**
   * Set the current master thread.
   * 
   * @param currentMasterThread
   */
  public void setCurrentMasterThread(Thread currentMasterThread) {
    mCurrentMasterThread = currentMasterThread;
  }

  public void start() throws IOException {
    mLeaderSelector.start();
  }

  @Override
  public void stateChanged(CuratorFramework client, ConnectionState newState) {
    mIsLeader.set(false);

    if ((newState == ConnectionState.LOST) || (newState == ConnectionState.SUSPENDED)) {
      if (mCurrentMasterThread != null) {
        mCurrentMasterThread.interrupt();
      }
    } else {
      try {
        LOG.info("The current leader is " + mLeaderSelector.getLeader().getId());
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }

  @Override
  public void takeLeadership(CuratorFramework client) throws Exception {
    mIsLeader.set(true);
    if (client.checkExists().forPath(mLeaderFolder + mName) != null) {
      client.delete().forPath(mLeaderFolder + mName);
    }
    client.create().creatingParentsIfNeeded().forPath(mLeaderFolder + mName);
    LOG.info(mName + " is now the leader.");
    try {
      while (true) {
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
      }
    } catch (InterruptedException e) {
      LOG.error(mName + " was interrupted.", e);
      Thread.currentThread().interrupt();
    } finally {
      mCurrentMasterThread = null;
      LOG.warn(mName + " relinquishing leadership.");
    }
    LOG.info("The current leader is " + mLeaderSelector.getLeader().getId());
    LOG.info("All partitations: " + mLeaderSelector.getParticipants());
    client.delete().forPath(mLeaderFolder + mName);
  }
}
