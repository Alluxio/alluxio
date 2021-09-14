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

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.conf.Hash;
import alluxio.conf.PropertyKey;
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
import alluxio.server.auth.ClusterInitializationIntegrationTest;
import alluxio.util.CommonUtils;

import org.apache.ratis.protocol.Message;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ResizingEBJTest extends BaseEmbeddedJournalTest {

  @Test
  public void resizeCluster() throws Exception {
    final int NUM_MASTERS = 5;
    final int NUM_WORKERS = 0;
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_RESIZE)
        .setClusterName("EmbeddedJournalResizing")
        .setNumMasters(NUM_MASTERS)
        .setNumWorkers(NUM_WORKERS)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED.toString())
        .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
        // To make the test run faster.
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "750ms")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1500ms")
        .build();
    mCluster.start();

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
    final int NUM_MASTERS = 2;
    final int NUM_WORKERS = 0;
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_GROW)
        .setClusterName("EmbeddedJournalAddMaster")
        .setNumMasters(NUM_MASTERS)
        .setNumWorkers(NUM_WORKERS)
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

    addNewMastersToCluster(PortCoordination.EMBEDDED_JOURNAL_GROW_NEW_MASTER);

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

  @Test
  public void shipOfTheseus() throws Exception {
    final int NUM_MASTERS = 5;
    final int NUM_WORKERS = 0;
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_THESEUS)
        .setClusterName("EmbeddedJournalAddMaster")
        .setNumMasters(NUM_MASTERS)
        .setNumWorkers(NUM_WORKERS)
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

    List<MasterNetAddress> originalMasters = mCluster.getMasterAddresses();
    for (int i = 0; i < originalMasters.size(); i++) {
      mCluster.stopMaster(i);
      mCluster.getJournalMasterClientForMaster().removeQuorumServer(
          masterEBJAddr2NetAddr(originalMasters.get(i)));
      mCluster.startNewMasters(1, false);
      Thread.sleep(3_000);
      assertEquals(5,
          mCluster.getJournalMasterClientForMaster().getQuorumInfo().getServerInfoList().size());
      fs = mCluster.getFileSystemClient();
      System.out.printf("killing master %s%n", originalMasters.get(i));
      assertTrue(fs.exists(testDir));
    }
    Set<NetAddress> collect =
        originalMasters.stream().map(this::masterEBJAddr2NetAddr).collect(Collectors.toSet());
    List<QuorumServerInfo> infoList = mCluster.getJournalMasterClientForMaster().getQuorumInfo()
        .getServerInfoList();
    // assert any unavailable masters are part of the original masters
    assertTrue(infoList.stream()
        .filter(info -> info.getServerState() == QuorumServerState.UNAVAILABLE)
        .allMatch(info -> collect.contains(info.getServerAddress()))
    );
    // assert the quorum remained the same size as the start
    assertEquals(NUM_MASTERS, infoList.stream()
        .filter(info -> info.getServerState() == QuorumServerState.AVAILABLE).count()
    );
    // assert that none of the currently available masters were part of the original masters
    assertTrue(infoList.stream()
        .filter(info -> info.getServerState() == QuorumServerState.AVAILABLE)
        .noneMatch(info -> collect.contains(info.getServerAddress()))
    );

    mCluster.notifySuccess();
  }

  private NetAddress masterEBJAddr2NetAddr(MasterNetAddress masterAddr) {
    return NetAddress.newBuilder().setHost(masterAddr.getHostname())
        .setRpcPort(masterAddr.getEmbeddedJournalPort()).build();
  }

  @Ignore
  @Test
  public void updateRaftGroup() throws Exception {
    final int NUM_MASTERS = 2;
    final int NUM_WORKERS = 0;
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_UPDATE_RAFT_GROUP)
        .setClusterName("EmbeddedJournalUpdateGroup")
        .setNumMasters(NUM_MASTERS)
        .setNumWorkers(NUM_WORKERS)
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
    Assert.assertEquals(NUM_MASTERS,
        mCluster.getJournalMasterClientForMaster().getQuorumInfo().getServerInfoList().size());

    addNewMastersToCluster(PortCoordination.EMBEDDED_JOURNAL_UPDATE_RAFT_GROUP_NEW);
    AlluxioMasterProcess newMaster = mNewMasters.get(mNewMasters.size() - 1);
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
            .toArray().length == NUM_MASTERS + 2;
      } catch (Exception exc) {
        throw new RuntimeException(exc);
      }
    });

    Assert.assertTrue(fs.exists(testDir));
    FileSystemMaster master = newMaster.getMaster(FileSystemMaster.class);
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
