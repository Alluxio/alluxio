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
import alluxio.LeaderSelectorClient;
import alluxio.PropertyKey;
import alluxio.master.journal.Journal;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The fault tolerant version of {@link AlluxioMaster} that uses zookeeper and standby masters.
 */
@NotThreadSafe
final class FaultTolerantAlluxioMaster extends DefaultAlluxioMaster {
  private static final Logger LOG = LoggerFactory.getLogger(FaultTolerantAlluxioMaster.class);

  /** The zookeeper client that handles selecting the leader. */
  private LeaderSelectorClient mLeaderSelectorClient = null;

  /**
   * Creates a {@link FaultTolerantAlluxioMaster}.
   */
  protected FaultTolerantAlluxioMaster() {
    Preconditions.checkArgument(Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));

    // Set up zookeeper specific functionality.
    try {
      // InetSocketAddress.toString causes test issues, so build the string by hand
      String zkName = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC) + ":"
          + getRpcAddress().getPort();
      String zkAddress = Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS);
      String zkElectionPath = Configuration.get(PropertyKey.ZOOKEEPER_ELECTION_PATH);
      String zkLeaderPath = Configuration.get(PropertyKey.ZOOKEEPER_LEADER_PATH);
      mLeaderSelectorClient =
          new LeaderSelectorClient(zkAddress, zkElectionPath, zkLeaderPath, zkName);

      // Check that the journal has been formatted.
      checkJournalFormatted();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void start() throws Exception {
    try {
      mLeaderSelectorClient.start();
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }

    Thread currentThread = Thread.currentThread();
    mLeaderSelectorClient.setCurrentMasterThread(currentThread);
    boolean started = false;

    while (!Thread.interrupted()) {
      if (mLeaderSelectorClient.isLeader()) {
        stopServing();
        stopMasters();
        // Transitioning from standby to leader, replace read-only journal with writable journal.
        for (Master master : mRegistry.getMasters()) {
          master.transitionToLeader();
        }
        startMasters(true);
        started = true;
        startServing("(gained leadership)", "(lost leadership)");
      } else {
        // This master should be standby, and not the leader
        if (isServing() || !started) {
          // Need to transition this master to standby mode.
          stopServing();
          stopMasters();

          // When transitioning from leader to standby, recreate the masters with a read-only
          // journal.
          createMasters(new Journal.Factory(getJournalLocation()));
          startMasters(false);
          started = true;
        }
        // This master is already in standby mode. No further actions needed.
      }

      CommonUtils.sleepMs(LOG, 100);
    }
  }

  @Override
  public void stop() throws Exception {
    super.stop();
    if (mLeaderSelectorClient != null) {
      mLeaderSelectorClient.close();
    }
  }
}
