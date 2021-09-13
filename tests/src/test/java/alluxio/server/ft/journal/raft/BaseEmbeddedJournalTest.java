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

package alluxio.server.ft.journal.raft;

import alluxio.AlluxioTestDirectory;
import alluxio.ConfigurationRule;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.QuorumServerState;
import alluxio.master.AlluxioMasterProcess;
import alluxio.master.journal.JournalType;
import alluxio.multi.process.MasterNetAddress;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.testutils.BaseIntegrationTest;

import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;

import org.junit.After;
import org.junit.Rule;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;

public class BaseEmbeddedJournalTest extends BaseIntegrationTest {

  protected static final int NUM_MASTERS = 5;
  protected static final int NUM_WORKERS = 0;

  @Rule
  public ConfigurationRule mConf =
          new ConfigurationRule(PropertyKey.USER_METRICS_COLLECTION_ENABLED, "false",
                  ServerConfiguration.global());

  public MultiProcessCluster mCluster;
  // Used to grow cluster.
  protected List<AlluxioMasterProcess> mNewMasters = new ArrayList<>();

  protected void standardBefore() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.allocate(NUM_MASTERS, NUM_WORKERS))
            .setClusterName(UUID.randomUUID().toString())
            .setNumMasters(NUM_MASTERS)
            .setNumWorkers(NUM_WORKERS)
            .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED.toString())
            .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
            .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "750ms")
            .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1500ms")
            .build();
    mCluster.start();
  }

  @After
  public void after() throws Exception {
    if (mCluster != null) {
      mCluster.destroy();
    }
    for (AlluxioMasterProcess newMasterProcess : mNewMasters) {
      newMasterProcess.stop();
    }
    mNewMasters.clear();
  }

  public AlluxioMasterProcess addNewMasterToCluster() throws Exception {
    // Create and start a new master to join to existing cluster.
    // Get new master address.
    List<PortCoordination.ReservedPort> allocated = PortCoordination.allocate(1, 0);
    MasterNetAddress newMasterAddress = new MasterNetAddress(
        NetworkAddressUtils.getLocalHostName(
            (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)),
        allocated.get(0).getPort(),
        allocated.get(1).getPort(),
        allocated.get(2).getPort());

    // Update RPC and EmbeddedJournal addresses with the new master address.
    String newBootstrapList = ServerConfiguration.get(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES)
        + "," + newMasterAddress.getHostname() + ":" + newMasterAddress.getEmbeddedJournalPort();
    String newRpcList = ServerConfiguration.get(PropertyKey.MASTER_RPC_ADDRESSES) + ","
        + newMasterAddress.getHostname() + ":" + newMasterAddress.getRpcPort();
    ServerConfiguration.global().set(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES,
        newBootstrapList);
    ServerConfiguration.global().set(PropertyKey.MASTER_RPC_ADDRESSES, newRpcList);

    // Create a separate working dir for the new master.
    File newMasterWorkDir =
        AlluxioTestDirectory.createTemporaryDirectory("EmbeddedJournalAddMaster-NewMaster");
    newMasterWorkDir.deleteOnExit();

    // Create journal dir for the new master and update configuration.
    File newMasterJournalDir = new File(newMasterWorkDir, "journal-newmaster");
    newMasterJournalDir.mkdirs();
    ServerConfiguration.global().set(PropertyKey.MASTER_JOURNAL_FOLDER,
        newMasterJournalDir.getAbsolutePath());

    // Update network settings for the new master.
    ServerConfiguration.global().set(PropertyKey.MASTER_HOSTNAME, newMasterAddress.getHostname());
    ServerConfiguration.global().set(PropertyKey.MASTER_RPC_PORT,
        Integer.toString(newMasterAddress.getRpcPort()));
    ServerConfiguration.global().set(PropertyKey.MASTER_EMBEDDED_JOURNAL_PORT,
        Integer.toString(newMasterAddress.getEmbeddedJournalPort()));

    // Create and start the new master.
    AlluxioMasterProcess newMaster = AlluxioMasterProcess.Factory.create();
    mNewMasters.add(newMaster);
    // Update cluster with the new address for further queries to
    // include the new master. Otherwise clients could fail if stopping
    // a master causes the new master to become the leader.
    mCluster.addExternalMasterAddress(newMasterAddress);

    // Submit a common task for starting the master.
    ForkJoinPool.commonPool().execute(() -> {
      try {
        newMaster.start();
      } catch (Exception e) {
        throw new RuntimeException("Failed to start new master.", e);
      }
    });

    // Wait until quorum size is increased to 3.
    CommonUtils.waitFor("New master is included in quorum", () -> {
      try {
        return mCluster.getJournalMasterClientForMaster().getQuorumInfo().getServerInfoList()
            .stream().filter(x -> x.getServerState() == QuorumServerState.AVAILABLE)
            .toArray().length == 3;
      } catch (Exception exc) {
        throw new RuntimeException(exc);
      }
    });

    return newMaster;
  }
}
