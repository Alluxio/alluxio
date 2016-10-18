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

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Masters use this client to elect a leader.
 */
@NotThreadSafe
public final class LeaderSelectorClient implements Closeable, LeaderSelectorListener {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The election path in Zookeeper. */
  private final String mElectionPath;
  /** The path of the leader in Zookeeper. */
  private final String mLeaderFolder;
  /** The LeaderSelector used to elect. */
  private final LeaderSelector mLeaderSelector;
  /** The name of this master in Zookeeper. */
  private final String mName;
  /** The address to Zookeeper. */
  private final String mZookeeperAddress;

  /** Whether this master is the leader master now. */
  private AtomicBoolean mIsLeader = new AtomicBoolean(false);
  /** The thread of the current master. */
  private volatile Thread mCurrentMasterThread = null;

  /**
   * Constructs a new {@link LeaderSelectorClient}.
   *
   * @param zookeeperAddress the address to Zookeeper
   * @param electionPath the election path
   * @param leaderPath the path of the leader
   * @param name the name
   */
  public LeaderSelectorClient(String zookeeperAddress, String electionPath, String leaderPath,
      String name) {
    mZookeeperAddress = zookeeperAddress;
    mElectionPath = electionPath;
    if (leaderPath.endsWith(AlluxioURI.SEPARATOR)) {
      mLeaderFolder = leaderPath;
    } else {
      mLeaderFolder = leaderPath + AlluxioURI.SEPARATOR;
    }
    mName = name;

    // Create a leader selector using the given path for management.
    // All participants in a given leader selection must use the same path.
    mLeaderSelector = new LeaderSelector(getNewCuratorClient(), mElectionPath, this);
    mLeaderSelector.setId(name);

    // For most cases you will want your instance to requeue when it relinquishes leadership.
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
      // TODO(hy): This should not happen in unit tests.
      if (!e.getMessage().equals("Already closed or has not been started")) {
        throw e;
      }
    }
  }

  /**
   * Gets the name of the leader.
   *
   * @return the leader name
   */
  public String getName() {
    return mName;
  }

  /**
   * Gets the participants.
   *
   * @return the list of participants
   */
  public List<String> getParticipants() {
    try {
      List<Participant> participants = new ArrayList<>(mLeaderSelector.getParticipants());
      List<String> results = new ArrayList<>();
      for (Participant part : participants) {
        results.add(part.getId());
      }
      return results;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  /**
   * Checks if the client is the leader.
   *
   * @return true if the client is the leader, false otherwise
   */
  public boolean isLeader() {
    return mIsLeader.get();
  }

  /**
   * @param currentMasterThread the thread to use as the master thread
   */
  public void setCurrentMasterThread(Thread currentMasterThread) {
    mCurrentMasterThread = Preconditions.checkNotNull(currentMasterThread);
  }

  /**
   * Starts the leader selection.
   *
   * @throws IOException if an error occurs during leader selection
   */
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
        LOG.info("The current leader is {}", mLeaderSelector.getLeader().getId());
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }

  @Override
  public void takeLeadership(CuratorFramework client) throws Exception {
    mIsLeader.set(true);
    if (client.checkExists().forPath(mLeaderFolder + mName) != null) {
      LOG.info("Deleting zk path: {}{}", mLeaderFolder, mName);
      client.delete().forPath(mLeaderFolder + mName);
    }
    LOG.info("Creating zk path: {}{}", mLeaderFolder, mName);
    client.create().creatingParentsIfNeeded().forPath(mLeaderFolder + mName);
    LOG.info("{} is now the leader.", mName);
    try {
      while (true) {
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
      }
    } catch (InterruptedException e) {
      LOG.error(mName + " was interrupted.", e);
      Thread.currentThread().interrupt();
    } finally {
      mIsLeader.set(false);
      mCurrentMasterThread = null;
      LOG.warn("{} relinquishing leadership.", mName);
      LOG.info("The current leader is {}", mLeaderSelector.getLeader().getId());
      LOG.info("All participants: {}", mLeaderSelector.getParticipants());
      client.delete().forPath(mLeaderFolder + mName);
    }
  }

  /**
   * Returns a new client for the zookeeper connection. The client is already started before
   * returning.
   *
   * @return a new {@link CuratorFramework} client to use for leader selection
   */
  private CuratorFramework getNewCuratorClient() {
    CuratorFramework client = CuratorFrameworkFactory.newClient(mZookeeperAddress,
        new ExponentialBackoffRetry(Constants.SECOND_MS, 3));
    client.start();

    // Sometimes, if the master crashes and restarts too quickly (faster than the zookeeper
    // timeout), zookeeper thinks the new client is still an old one. In order to ensure a clean
    // state, explicitly close the "old" client recreate a new one.
    client.close();
    client = CuratorFrameworkFactory.newClient(mZookeeperAddress,
        new ExponentialBackoffRetry(Constants.SECOND_MS, 3));
    client.start();
    return client;
  }
}
