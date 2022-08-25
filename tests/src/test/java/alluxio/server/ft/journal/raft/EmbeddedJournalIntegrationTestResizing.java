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
import alluxio.grpc.NetAddress;
import alluxio.grpc.QuorumServerInfo;
import alluxio.grpc.QuorumServerState;
import alluxio.multi.process.MasterNetAddress;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.util.CommonUtils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class EmbeddedJournalIntegrationTestResizing extends EmbeddedJournalIntegrationTestBase {

  @Test
  public void resizeCluster() throws Exception {
    final int NUM_MASTERS = 5;
    final int NUM_WORKERS = 0;
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_RESIZE)
        .setClusterName("EmbeddedJournalResizing_resizeCluster")
        .setNumMasters(NUM_MASTERS)
        .setNumWorkers(NUM_WORKERS)
        .addProperties(mDefaultProperties)
        .build();
    mCluster.start();

    assertEquals(5,
        mCluster.getJournalMasterClientForMaster().getQuorumInfo().getServerInfoList().size());

    AlluxioURI testDir = new AlluxioURI("/" + CommonUtils.randomAlphaNumString(10));
    FileSystem fs = mCluster.getFileSystemClient();
    fs.createDirectory(testDir);
    assertTrue(fs.exists(testDir));

    // Stop 2 masters. Now cluster can't tolerate any loss.
    mCluster.stopMaster(0);
    mCluster.stopMaster(1);
    // Verify cluster is still serving requests.
    assertTrue(fs.exists(testDir));

    waitForQuorumPropertySize(info -> info.getServerState() == QuorumServerState.UNAVAILABLE, 2);

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
        .setClusterName("EmbeddedJournalResizing_growCluster")
        .setNumMasters(NUM_MASTERS)
        .setNumWorkers(NUM_WORKERS)
        .addProperties(mDefaultProperties)
        .build();
    mCluster.start();

    AlluxioURI testDir = new AlluxioURI("/" + CommonUtils.randomAlphaNumString(10));
    FileSystem fs = mCluster.getFileSystemClient();
    fs.createDirectory(testDir);
    assertTrue(fs.exists(testDir));

    // Validate current quorum size.
    assertEquals(2,
        mCluster.getJournalMasterClientForMaster().getQuorumInfo().getServerInfoList().size());

    mCluster.startNewMasters(1, false);
    waitForQuorumPropertySize(info -> info.getServerState() == QuorumServerState.AVAILABLE, 3);

    // Reacquire FS client after cluster grew.
    fs = mCluster.getFileSystemClient();

    // Verify cluster is still operational.
    assertTrue(fs.exists(testDir));

    // Stop a master on the initial cluster.
    // With the addition of a new master, cluster should now be able to tolerate single master loss.
    mCluster.stopAndRemoveMaster(0);

    // Wait until cluster registers unavailability of the shut down master.
    waitForQuorumPropertySize(info -> info.getServerState() == QuorumServerState.UNAVAILABLE, 1);

    // Verify cluster is still operational.
    assertTrue(fs.exists(testDir));

    mCluster.notifySuccess();
  }

  @Test
  public void replaceAll() throws Exception {
    final int NUM_MASTERS = 3;
    final int NUM_WORKERS = 0;
    // reusing ports
    ArrayList<PortCoordination.ReservedPort> ports =
        new ArrayList<>(PortCoordination.EMBEDDED_JOURNAL_REPLACE_ALL);
    ports.addAll(PortCoordination.EMBEDDED_JOURNAL_REPLACE_ALL);
    mCluster = MultiProcessCluster.newBuilder(ports)
        .setClusterName("EmbeddedJournalResizing_replaceAll")
        .setNumMasters(NUM_MASTERS)
        .setNumWorkers(NUM_WORKERS)
        .addProperties(mDefaultProperties)
        .build();
    mCluster.start();

    AlluxioURI testDir = new AlluxioURI("/" + CommonUtils.randomAlphaNumString(10));
    FileSystem fs = mCluster.getFileSystemClient();
    fs.createDirectory(testDir);
    assertTrue(fs.exists(testDir));

    List<MasterNetAddress> originalMasters = new ArrayList<>(mCluster.getMasterAddresses());
    for (MasterNetAddress masterNetAddress : originalMasters) {
      // remove a master from the Alluxio cluster (could be the leader)
      int masterIdx = mCluster.getMasterAddresses().indexOf(masterNetAddress);
      mCluster.stopAndRemoveMaster(masterIdx);
      waitForQuorumPropertySize(info -> info.getServerState() == QuorumServerState.UNAVAILABLE, 1);
      // start a new master to replace the lost master
      mCluster.startNewMasters(1, false);
      waitForQuorumPropertySize(info -> info.getServerState() == QuorumServerState.UNAVAILABLE, 0);
      // verify that the cluster is still operational
      fs = mCluster.getFileSystemClient();
      assertTrue(fs.exists(testDir));
    }

    mCluster.notifySuccess();
  }
}
