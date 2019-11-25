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

import alluxio.AlluxioURI;
import alluxio.conf.ServerConfiguration;
import alluxio.Constants;
import alluxio.conf.PropertyKey;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.SessionConnectionStateErrorPolicy;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Masters use this client to elect a leader.
 */
@NotThreadSafe
public final class PrimarySelectorClient extends AbstractPrimarySelector
    implements Closeable, LeaderSelectorListener {
  private static final Logger LOG = LoggerFactory.getLogger(PrimarySelectorClient.class);

  /** A constant session Id for when selector is not a leader. */
  private static final int NOT_A_LEADER = -1;

  /** The election path in Zookeeper. */
  private final String mElectionPath;
  /** The path of the leader in Zookeeper. */
  private final String mLeaderFolder;
  /** The PrimarySelector used to elect. */
  private final LeaderSelector mLeaderSelector;
  /** The name of this master in Zookeeper. */
  private String mName;
  /** The address to Zookeeper. */
  private final String mZookeeperAddress;
  /** The sessionID under which leadership is granted. */
  private long mLeaderZkSessionId;
  /** Configured connection error policy for leader election. */
  private ZookeeperConnectionErrorPolicy mConnectionErrorPolicy;

  /**
   * Constructs a new {@link PrimarySelectorClient}.
   *
   * @param zookeeperAddress the address to Zookeeper
   * @param electionPath the election path
   * @param leaderPath the path of the leader
   */
  public PrimarySelectorClient(String zookeeperAddress, String electionPath, String leaderPath) {
    mZookeeperAddress = zookeeperAddress;
    mElectionPath = electionPath;
    if (leaderPath.endsWith(AlluxioURI.SEPARATOR)) {
      mLeaderFolder = leaderPath;
    } else {
      mLeaderFolder = leaderPath + AlluxioURI.SEPARATOR;
    }
    mConnectionErrorPolicy = ServerConfiguration.getEnum(
        PropertyKey.ZOOKEEPER_LEADER_CONNECTION_ERROR_POLICY, ZookeeperConnectionErrorPolicy.class);

    mLeaderZkSessionId = NOT_A_LEADER;

    // Create a leader selector using the given path for management.
    // All participants in a given leader selection must use the same path.
    mLeaderSelector = new LeaderSelector(getNewCuratorClient(), mElectionPath, this);

    // For most cases you will want your instance to requeue when it relinquishes leadership.
    mLeaderSelector.autoRequeue();
  }

  @Override
  public void close() throws IOException {
    try {
      mLeaderSelector.close();
    } catch (IllegalStateException e) {
      // TODO(hy): This should not happen in unit tests.
      if (!e.getMessage().equals("Already closed or has not been started")) {
        throw e;
      }
    }
  }

  @Override
  public void stop() throws IOException {
    close();
  }

  /**
   * @return the leader name
   */
  public String getName() {
    return mName;
  }

  /**
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
   * Starts the leader selection. If the leader selector client loses connection to Zookeeper or
   * gets closed, the calling thread will be interrupted.
   */
  @Override
  public void start(InetSocketAddress address) throws IOException {
    mName = address.getHostName() + ":" + address.getPort();
    mLeaderSelector.setId(mName);
    mLeaderSelector.start();
  }

  @Override
  public void stateChanged(CuratorFramework client, ConnectionState newState) {
    // Handle state change based on configured connection error policy.
    if (mConnectionErrorPolicy == ZookeeperConnectionErrorPolicy.SESSION) {
      handleStateChangeSession(client, newState);
    } else {
      handleStateChangeStandard(client, newState);
    }

    if ((newState != ConnectionState.LOST) && (newState != ConnectionState.SUSPENDED)) {
      try {
        String leaderId = mLeaderSelector.getLeader().getId();
        if (!leaderId.isEmpty()) {
          LOG.info("The current leader is {}", leaderId);
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }

  /**
   * Used to handle state change under STANDARD connection error policy.
   */
  private void handleStateChangeStandard(CuratorFramework client, ConnectionState newState) {
    setState(State.SECONDARY);
  }

  /**
   * Used to handle state change under SESSION connection error policy.
   */
  private void handleStateChangeSession(CuratorFramework client, ConnectionState newState) {
    // Handle state change.
    switch (newState) {
      case CONNECTED:
      case LOST:
        setState(State.SECONDARY);
        break;
      case SUSPENDED:
        break;
      case RECONNECTED:
        // Try to retain existing PRIMARY role under session policy.
        if (getState() == State.PRIMARY) {
          /**
           * Do a sanity check when reconnected for a selector with "PRIMARY" mode. This is to
           * ensure that curator reconnected with the same Id. Hence, guaranteeing Zookeeper state
           * for this instance was preserved.
           */
          try {
            long reconnectSessionId = client.getZookeeperClient().getZooKeeper().getSessionId();
            if (mLeaderZkSessionId != reconnectSessionId) {
              LOG.warn(String.format(
                  "Curator reconnected under a different session. "
                      + "Old sessionId: %x, New sessionId: %x",
                  mLeaderZkSessionId, reconnectSessionId));
              setState(State.SECONDARY);
            } else {
              LOG.info(String.format(
                  "Retaining leader state after zookeeper reconnected with sessionId: %x.",
                  reconnectSessionId));
            }
          } catch (Exception e) {
            LOG.warn("Cannot query session Id after session is reconnected.", e);
            setState(State.SECONDARY);
          }
        }
        break;
      default:
        throw new IllegalStateException(String.format("Unexpected state: %s", newState));
    }
  }

  @Override
  public void takeLeadership(CuratorFramework client) throws Exception {
    setState(State.PRIMARY);
    if (client.checkExists().forPath(mLeaderFolder + mName) != null) {
      LOG.info("Deleting zk path: {}{}", mLeaderFolder, mName);
      client.delete().forPath(mLeaderFolder + mName);
    }
    LOG.info("Creating zk path: {}{}", mLeaderFolder, mName);
    client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
        .forPath(mLeaderFolder + mName);
    LOG.info("{} is now the leader.", mName);
    try {
      mLeaderZkSessionId = client.getZookeeperClient().getZooKeeper().getSessionId();
      LOG.info(String.format("Taken leadership under session Id: %x", mLeaderZkSessionId));
      waitForState(State.SECONDARY);
    } finally {
      LOG.warn("{} relinquishing leadership.", mName);
      LOG.info("The current leader is {}", mLeaderSelector.getLeader().getId());
      LOG.info("All participants: {}", mLeaderSelector.getParticipants());
      client.delete().forPath(mLeaderFolder + mName);
      mLeaderZkSessionId = NOT_A_LEADER;
    }
  }

  /**
   * Returns a new client for the zookeeper connection. The client is already started before
   * returning.
   *
   * @return a new {@link CuratorFramework} client to use for leader selection
   */
  private CuratorFramework getNewCuratorClient() {
    LOG.info("Creating new zookeeper client for primary selector {}", mZookeeperAddress);
    CuratorFrameworkFactory.Builder curatorBuilder = CuratorFrameworkFactory.builder();
    curatorBuilder.connectString(mZookeeperAddress);
    curatorBuilder.retryPolicy(new ExponentialBackoffRetry(Constants.SECOND_MS, 3));
    curatorBuilder
        .sessionTimeoutMs((int) ServerConfiguration.getMs(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT));
    curatorBuilder.connectionTimeoutMs(
        (int) ServerConfiguration.getMs(PropertyKey.ZOOKEEPER_CONNECTION_TIMEOUT));
    // Force compatibility mode to support writing to 3.4.x servers.
    curatorBuilder.zk34CompatibilityMode(true);
    // Prevent using container parents as it breaks compatibility with 3.4.x servers.
    // This is only required if the client is used to write data to zookeeper.
    curatorBuilder.dontUseContainerParents();
    // Use SESSION policy for leader connection errors, when configured.
    if (mConnectionErrorPolicy == ZookeeperConnectionErrorPolicy.SESSION) {
      curatorBuilder.connectionStateErrorPolicy(new SessionConnectionStateErrorPolicy());
    }

    CuratorFramework client = curatorBuilder.build();
    client.start();

    // Sometimes, if the master crashes and restarts too quickly (faster than the zookeeper
    // timeout), zookeeper thinks the new client is still an old one. In order to ensure a clean
    // state, explicitly close the "old" client and recreate a new one.
    client.close();

    client = curatorBuilder.build();
    client.start();
    return client;
  }

  /**
   * Defines supported connection error policies for leader election.
   */
  protected enum ZookeeperConnectionErrorPolicy {
    STANDARD, // Treat each state change as error.
    SESSION   // Treat state change as error when session is lost.
  }
}
