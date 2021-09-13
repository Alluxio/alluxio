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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.NetAddress;
import alluxio.grpc.QuorumServerInfo;
import alluxio.grpc.QuorumServerState;
import alluxio.master.AlluxioMasterProcess;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.multi.process.MasterNetAddress;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;

import org.apache.ratis.protocol.Message;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

public class ResizingEBJTest extends BaseEmbeddedJournalTest {

  @Test
  public void resizeCluster() throws Exception {
    standardBefore();

    assertEquals(5,
        mCluster.getJournalMasterClientForMaster().getQuorumInfo().getServerInfoList().size());

    AlluxioURI testDir = new AlluxioURI("/dir");
    FileSystem fs = mCluster.getFileSystemClient();
    fs.createDirectory(testDir);
    assertTrue(fs.exists(testDir));

    // Stop 2 masters. Now cluster can't tolerate any loss.
    mCluster.stopMaster(0);
    mCluster.stopMaster(1);
    // Verify cluster is still serving requests.
    assertTrue(fs.exists(testDir));

    CommonUtils.waitFor("Quorum noticing master unavailability", () -> {
      try {
        int unavailableCount = 0;
        for (QuorumServerInfo serverState : mCluster.getJournalMasterClientForMaster()
            .getQuorumInfo().getServerInfoList()) {
          if (serverState.getServerState().equals(QuorumServerState.UNAVAILABLE)) {
            unavailableCount++;
          }
        }
        return unavailableCount >= 2;
      } catch (Exception exc) {
        throw new RuntimeException(exc);
      }
    });

    // Get and verify list of unavailable masters.
    List<NetAddress> unavailableMasters = new LinkedList<>();
    for (QuorumServerInfo serverState : mCluster.getJournalMasterClientForMaster().getQuorumInfo()
        .getServerInfoList()) {
      if (serverState.getServerState().equals(QuorumServerState.UNAVAILABLE)) {
        unavailableMasters.add(serverState.getServerAddress());
      }
    }
    assertEquals(2, unavailableMasters.size());

    // Remove unavailable masters from quorum.
    for (NetAddress unavailableMasterAddress : unavailableMasters) {
      mCluster.getJournalMasterClientForMaster().removeQuorumServer(unavailableMasterAddress);
    }
    // Verify quorum is down to 3 masters.
    assertEquals(3,
        mCluster.getJournalMasterClientForMaster().getQuorumInfo().getServerInfoList().size());

    // Validate that cluster can tolerate one more master failure after resizing.
    mCluster.stopMaster(2);
    assertTrue(fs.exists(testDir));
    mCluster.notifySuccess();
  }

  @Test
  public void growCluster() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_GROW)
        .setClusterName("EmbeddedJournalAddMaster").setNumMasters(2).setNumWorkers(0)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED.toString())
        .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
        // To make the test run faster.
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "2s")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "4s")
        .build();
    mCluster.start();

    AlluxioURI testDir = new AlluxioURI("/" + CommonUtils.randomAlphaNumString(10));
    FileSystem fs = mCluster.getFileSystemClient();
    fs.createDirectory(testDir);
    Assert.assertTrue(fs.exists(testDir));

    // Validate current quorum size.
    Assert.assertEquals(2,
        mCluster.getJournalMasterClientForMaster().getQuorumInfo().getServerInfoList().size());

    // Create and start a new master to join to existing cluster.
    // Get new master address.
    MasterNetAddress newMasterAddress = new MasterNetAddress(
        NetworkAddressUtils.getLocalHostName(
            (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)),
        PortCoordination.EMBEDDED_JOURNAL_GROW_NEWMASTER.get(0).getPort(),
        PortCoordination.EMBEDDED_JOURNAL_GROW_NEWMASTER.get(1).getPort(),
        PortCoordination.EMBEDDED_JOURNAL_GROW_NEWMASTER.get(2).getPort());

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
    mNewMaster = AlluxioMasterProcess.Factory.create();
    // Update cluster with the new address for further queries to
    // include the new master. Otherwise clients could fail if stopping
    // a master causes the new master to become the leader.
    mCluster.addExternalMasterAddress(newMasterAddress);

    // Submit a common task for starting the master.
    ForkJoinPool.commonPool().execute(() -> {
      try {
        mNewMaster.start();
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

    // Reacquire FS client after cluster grew.
    fs = mCluster.getFileSystemClient();

    // Verify cluster is still operational.
    Assert.assertTrue(fs.exists(testDir));

    // Stop a master on the initial cluster.
    // With the addition of a new master, cluster should now be able to tolerate single master loss.
    mCluster.stopMaster(0);

    // Wait until cluster registers unavailability of the shut down master.
    CommonUtils.waitFor("Quorum noticing master unavailability", () -> {
      try {
        int unavailableCount = 0;
        for (QuorumServerInfo serverState : mCluster.getJournalMasterClientForMaster()
            .getQuorumInfo().getServerInfoList()) {
          if (serverState.getServerState().equals(QuorumServerState.UNAVAILABLE)) {
            unavailableCount++;
          }
        }
        return unavailableCount == 1;
      } catch (Exception exc) {
        throw new RuntimeException(exc);
      }
    });

    // Verify cluster is still operational.
    Assert.assertTrue(fs.exists(testDir));

    mCluster.notifySuccess();
  }

  @Ignore
  @Test
  public void updateRaftGroup() throws Exception {
    int masterCount = 2;
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_UPDATE_RAFT_GROUP)
        .setClusterName("EmbeddedJournalUpdateGroup").setNumMasters(masterCount).setNumWorkers(0)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED.toString())
        .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
        .addProperty(PropertyKey.MASTER_METASTORE, "HEAP")
        // To make the test run faster.
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "2s")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "4s")
        .build();
    mCluster.start();

    AlluxioURI testDir = new AlluxioURI("/" + CommonUtils.randomAlphaNumString(10));
    FileSystem fs = mCluster.getFileSystemClient();
    fs.createDirectory(testDir);
    Assert.assertTrue(fs.exists(testDir));

    // Validate current quorum size.
    Assert.assertEquals(masterCount,
        mCluster.getJournalMasterClientForMaster().getQuorumInfo().getServerInfoList().size());

    // Create and start a new master to join to existing cluster.
    // Get new master address.
    MasterNetAddress newMasterAddress = new MasterNetAddress(
        NetworkAddressUtils.getLocalHostName(
            (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)),
        PortCoordination.EMBEDDED_JOURNAL_UPDATE_RAFT_GROUP_NEW.get(0).getPort(),
        PortCoordination.EMBEDDED_JOURNAL_UPDATE_RAFT_GROUP_NEW.get(1).getPort(),
        PortCoordination.EMBEDDED_JOURNAL_UPDATE_RAFT_GROUP_NEW.get(2).getPort());

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
        AlluxioTestDirectory.createTemporaryDirectory("EmbeddedJournalUpdateGroup-NewMaster");
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
    mNewMaster = AlluxioMasterProcess.Factory.create();
    // Update cluster with the new address for further queries to
    // include the new master. Otherwise clients could fail if stopping
    // a master causes the new master to become the leader.
    mCluster.addExternalMasterAddress(newMasterAddress);

    // Submit a common task for starting the master.
    ForkJoinPool.commonPool().execute(() -> {
      try {
        mNewMaster.start();
      } catch (Exception e) {
        throw new RuntimeException("Failed to start new master.", e);
      }
    });

    // Wait until quorum size is increased to 3.
    CommonUtils.waitFor("New master is included in quorum", () -> {
      try {
        return mCluster.getJournalMasterClientForMaster().getQuorumInfo().getServerInfoList()
            .stream().filter(x -> x.getServerState() == QuorumServerState.AVAILABLE)
            .toArray().length == masterCount + 1;
      } catch (Exception exc) {
        throw new RuntimeException(exc);
      }
    });

    // Reacquire FS client after cluster grew.
    fs = mCluster.getFileSystemClient();

    // Verify cluster is still operational.
    Assert.assertTrue(fs.exists(testDir));

    // start one more master
    mCluster.startNewMasters(1, false);

    // Wait until quorum size equals to new master count.
    CommonUtils.waitFor("New master is included in quorum", () -> {
      try {
        return mCluster.getJournalMasterClientForMaster().getQuorumInfo().getServerInfoList()
            .stream().filter(x -> x.getServerState() == QuorumServerState.AVAILABLE)
            .toArray().length == masterCount + 2;
      } catch (Exception exc) {
        throw new RuntimeException(exc);
      }
    });

    Assert.assertTrue(fs.exists(testDir));
    FileSystemMaster master = mNewMaster.getMaster(FileSystemMaster.class);
    RaftJournalSystem journal = ((RaftJournalSystem) master.getMasterContext().getJournalSystem());
    boolean error = journal.getCurrentGroup().getPeers().stream().anyMatch(
        peer -> {
          try {
            journal.sendMessageAsync(peer.getId(), Message.EMPTY).get();
            return false;
          } catch (Exception e) {
            e.printStackTrace();
            return true;
          }
        }
    );
    Assert.assertFalse("error send message to peers", error);
    mCluster.notifySuccess();
  }
}
