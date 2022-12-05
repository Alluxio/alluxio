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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.JournalDomain;
import alluxio.grpc.NodeState;
import alluxio.master.journal.JournalSystem;
import alluxio.master.service.SimpleService;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is responsible for initializing the different masters that are configured to run.
 */
@NotThreadSafe
public abstract class AlluxioSimpleMasterProcess extends MasterProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioSimpleMasterProcess.class);

  /** The master name. */
  final String mMasterName;

  /** The master journal domain. */
  final JournalDomain mJournalDomain;

  /**
   * The base class for running a master process that runs a single master implementing
   * the {@link AbstractMaster} interface.
   * @param masterName the name of the master (used when printing log messages)
   * @param journalDomain the journal domain of the master
   * @param journalSystem the journal system
   * @param leaderSelector the leader selector
   * @param webService the web service information
   * @param rpcService the rpc service information
   * @param hostNameKey the property key used to get the host name of this master
   */
  AlluxioSimpleMasterProcess(String masterName, JournalDomain journalDomain,
      JournalSystem journalSystem, PrimarySelector leaderSelector, ServiceType webService,
      ServiceType rpcService, PropertyKey hostNameKey) {
    super(journalSystem, leaderSelector, webService, rpcService);
    mMasterName = masterName;
    mJournalDomain = journalDomain;
    if (!Configuration.isSet(hostNameKey)) {
      Configuration.set(hostNameKey,
          NetworkAddressUtils.getLocalHostName(
              (int) Configuration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)));
    }
    try {
      if (!mJournalSystem.isFormatted()) {
        mJournalSystem.format();
      }
    } catch (Exception e) {
      LOG.error("Failed to create {} master", mMasterName, e);
      throw new RuntimeException(String.format("Failed to create %s master", mMasterName), e);
    }
  }

  @Override
  public void start() throws Exception {
    mServices.forEach(SimpleService::start);
    mJournalSystem.start();
    // the leader selector is created in state STANDBY. Once mLeaderSelector.start is called, it
    // can transition to PRIMARY at any point.
    mLeaderSelector.start(getRpcAddress());

    while (!Thread.interrupted()) {
      // We are in standby mode. Nothing to do until we become the primary.
      mLeaderSelector.waitForState(NodeState.PRIMARY);
      LOG.info("Transitioning from standby to primary");
      mJournalSystem.gainPrimacy();
      stopMasters();
      LOG.info("Secondary stopped");
      startMasterComponents(true);
      mServices.forEach(SimpleService::promote);
      LOG.info("Primary started");
      // We are in primary mode. Nothing to do until we become the standby.
      mLeaderSelector.waitForState(NodeState.STANDBY);
      LOG.info("Transitioning from primary to standby");
      mServices.forEach(SimpleService::demote);
      stopMasters();
      mJournalSystem.losePrimacy();
      LOG.info("Primary stopped");
      startMasterComponents(false);
      LOG.info("Standby started");
    }
  }

  /**
   * Stops the Alluxio master server.
   *
   * @throws Exception if stopping the master fails
   */
  @Override
  public void stop() throws Exception {
    mServices.forEach(SimpleService::stop);
    mJournalSystem.stop();
    stopMasters();
    mLeaderSelector.stop();
  }

  protected void startMasterComponents(boolean isLeader) {
    try {
      mRegistry.start(isLeader);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  protected void stopMasters() {
    try {
      mRegistry.stop();
    } catch (IOException e) {
      LOG.error("Failed to stop {} master", mMasterName, e);
      throw new RuntimeException(String.format("Failed to stop %s master", mMasterName), e);
    }
  }

  @Override
  public String toString() {
    return String.format("Alluxio %s master @ %s", mMasterName, mRpcConnectAddress);
  }
}
