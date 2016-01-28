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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Masters use this client to elect a leader.
 */
@NotThreadSafe
public final class LeaderSelectorClient implements Closeable, LeaderSelectorListener {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final String mElectionPath;
  private final String mLeaderFolder;
  private final LeaderSelector mLeaderSelector;
  private final String mName;
  private final String mZookeeperAddress;

  private AtomicBoolean mIsLeader = new AtomicBoolean(false);
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
    if (leaderPath.endsWith(TachyonURI.SEPARATOR)) {
      mLeaderFolder = leaderPath;
    } else {
      mLeaderFolder = leaderPath + TachyonURI.SEPARATOR;
    }
    mName = name;

    // Create a leader selector using the given path for management.
    // All participants in a given leader selection must use the same path.
    mLeaderSelector = new LeaderSelector(getNewCuratorClient(), mElectionPath, this);
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

  /**
   * Checks if the client is the leader.
   *
   * @return true if the client is the leader, false otherwise
   */
  public boolean isLeader() {
    return mIsLeader.get();
  }

  /**
   * Sets the current master thread.
   *
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
      LOG.info("deleting zk path: {}{}", mLeaderFolder, mName);
      client.delete().forPath(mLeaderFolder + mName);
    }
    LOG.info("creating zk path: {}{}", mLeaderFolder, mName);
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
