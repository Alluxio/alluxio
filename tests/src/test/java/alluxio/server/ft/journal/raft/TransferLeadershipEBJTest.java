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

import alluxio.conf.PropertyKey;
import alluxio.grpc.GetQuorumInfoPResponse;
import alluxio.grpc.NetAddress;
import alluxio.grpc.QuorumServerState;
import alluxio.master.journal.JournalType;
import alluxio.multi.process.MasterNetAddress;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;

import net.bytebuddy.utility.RandomString;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class TransferLeadershipEBJTest extends BaseEmbeddedJournalTest {

  private static final int MASTER_INDEX_WAIT_TIME = 5_000; // milliseconds
  public static final int NUM_MASTERS = 5;
  public static final int NUM_WORKERS = 0;

  @Before
  public void before() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_TRANSFER_LEADER)
        .setClusterName("TransferLeadership-" + RandomString.make(8))
        .setNumMasters(NUM_MASTERS)
        .setNumWorkers(NUM_WORKERS)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED.toString())
        .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "750ms")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1500ms")
        .build();
    mCluster.start();
  }

  @Test
  public void transferLeadership() throws Exception {
    int newLeaderIdx = (mCluster.getPrimaryMasterIndex(MASTER_INDEX_WAIT_TIME) + 1) % NUM_MASTERS;
    // `getPrimaryMasterIndex` uses the same `mMasterAddresses` variable as getMasterAddresses
    // we can therefore access to the new leader's address this way
    MasterNetAddress newLeaderAddr = mCluster.getMasterAddresses().get(newLeaderIdx);
    transferAndWait(newLeaderAddr);

    mCluster.notifySuccess();
  }

  @Test
  public void repeatedTransferLeadership() throws Exception {
    for (int i = 0; i < NUM_MASTERS; i++) {
      int newLeaderIdx = (mCluster.getPrimaryMasterIndex(MASTER_INDEX_WAIT_TIME) + 1) % NUM_MASTERS;
      // `getPrimaryMasterIndex` uses the same `mMasterAddresses` variable as getMasterAddresses
      // we can therefore access to the new leader's address this way
      MasterNetAddress newLeaderAddr = mCluster.getMasterAddresses().get(newLeaderIdx);
      transferAndWait(newLeaderAddr);
    }
    mCluster.notifySuccess();
  }

  @Test
  public void ensureAutoResetPriorities() throws Exception {
    for (int i = 0; i < NUM_MASTERS; i++) {
      int newLeaderIdx = (mCluster.getPrimaryMasterIndex(MASTER_INDEX_WAIT_TIME) + 1) % NUM_MASTERS;
      // `getPrimaryMasterIndex` uses the same `mMasterAddresses` variable as getMasterAddresses
      // we can therefore access to the new leader's address this way
      MasterNetAddress newLeaderAddr = mCluster.getMasterAddresses().get(newLeaderIdx);
      transferAndWait(newLeaderAddr);

      GetQuorumInfoPResponse info = mCluster.getJournalMasterClientForMaster().getQuorumInfo();
      // confirms that master priorities get reset to 0 for the new leading master and 1 for the
      // follower masters (this behavior is default within Apache Ratis 2.0)
      Assert.assertTrue(info.getServerInfoList().stream().allMatch(masterInfo ->
              masterInfo.getPriority() == (masterInfo.getIsLeader() ? 0 : 1)
      ));
    }
    mCluster.notifySuccess();
  }

  @Test
  public void transferLeadershipWhenAlreadyTransferring() throws Exception {
    int newLeaderIdx = (mCluster.getPrimaryMasterIndex(MASTER_INDEX_WAIT_TIME) + 1) % NUM_MASTERS;
    // `getPrimaryMasterIndex` uses the same `mMasterAddresses` variable as getMasterAddresses
    // we can therefore access to the new leader's address this way
    MasterNetAddress newLeaderAddr = mCluster.getMasterAddresses().get(newLeaderIdx);
    NetAddress netAddress = NetAddress.newBuilder().setHost(newLeaderAddr.getHostname())
            .setRpcPort(newLeaderAddr.getEmbeddedJournalPort()).build();

    mCluster.getJournalMasterClientForMaster().transferLeadership(netAddress);
    try {
      // this second call should throw an exception
      mCluster.getJournalMasterClientForMaster().transferLeadership(netAddress);
      Assert.fail("Should have thrown exception");
    } catch (IOException ioe) {
      // expected exception thrown
    }
    mCluster.notifySuccess();
  }

  @Test
  public void transferLeadershipOutsideCluster() throws Exception {
    NetAddress netAddress = NetAddress.newBuilder().setHost("hostname").setRpcPort(0).build();

    try {
      mCluster.getJournalMasterClientForMaster().transferLeadership(netAddress);
      Assert.fail("Should have thrown exception");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().startsWith(String.format("<%s:%d> is not part of the quorum",
              netAddress.getHost(), netAddress.getRpcPort())));

      for (MasterNetAddress address : mCluster.getMasterAddresses()) {
        String host = address.getHostname();
        int port = address.getEmbeddedJournalPort();
        Assert.assertTrue(e.getMessage().contains(String.format("%s:%d", host, port)));
      }
    }
    mCluster.notifySuccess();
  }

  @Test
  public void transferLeadershipToNewMember() throws Exception {
    mCluster.startNewMasters(1, false);
    waitForQuorumPropertySize(info -> info.getServerState() == QuorumServerState.AVAILABLE,
        NUM_MASTERS + 1);
    MasterNetAddress newLeaderAddr = mCluster.getMasterAddresses().get(NUM_MASTERS);
    transferAndWait(newLeaderAddr);
    mCluster.notifySuccess();
  }

  @Test
  public void transferLeadershipToUnavailableMaster() throws Exception {
    int newLeaderIdx = (mCluster.getPrimaryMasterIndex(MASTER_INDEX_WAIT_TIME) + 1) % NUM_MASTERS;
    // `getPrimaryMasterIndex` uses the same `mMasterAddresses` variable as getMasterAddresses
    // we can therefore access to the new leader's address this way
    MasterNetAddress newLeaderAddr = mCluster.getMasterAddresses().get(newLeaderIdx);

    mCluster.stopMaster(newLeaderIdx);

    try {
      transferAndWait(newLeaderAddr);
      Assert.fail("Transfer should have failed");
    } catch (TimeoutException e) {
      // expected exception
    }
    mCluster.notifySuccess();
  }

  private void transferAndWait(MasterNetAddress newLeaderAddr) throws Exception {
    NetAddress netAddress = NetAddress.newBuilder().setHost(newLeaderAddr.getHostname())
        .setRpcPort(newLeaderAddr.getEmbeddedJournalPort()).build();
    mCluster.getJournalMasterClientForMaster().transferLeadership(netAddress);

    waitForQuorumPropertySize(info -> info.getIsLeader()
        && info.getServerAddress().equals(netAddress), 1);
  }
}
