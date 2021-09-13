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

import alluxio.grpc.GetQuorumInfoPResponse;
import alluxio.grpc.NetAddress;
import alluxio.multi.process.MasterNetAddress;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TransferLeadershipEBJTest extends BaseEmbeddedJournalTest {

  private static final int MASTER_INDEX_WAIT_TIME = 5_000; // milliseconds

  @Before
  public void before() throws Exception {
    standardBefore();
  }

  @Test
  public void transferLeadership() throws Exception {
    int newLeaderIdx = (mCluster.getPrimaryMasterIndex(MASTER_INDEX_WAIT_TIME) + 1) % NUM_MASTERS;
    // `getPrimaryMasterIndex` uses the same `mMasterAddresses` variable as getMasterAddresses
    // we can therefore access to the new leader's address this way
    MasterNetAddress newLeaderAddr = mCluster.getMasterAddresses().get(newLeaderIdx);
    NetAddress netAddress = NetAddress.newBuilder().setHost(newLeaderAddr.getHostname())
            .setRpcPort(newLeaderAddr.getEmbeddedJournalPort()).build();

    mCluster.getJournalMasterClientForMaster().transferLeadership(netAddress);

    CommonUtils.waitFor("leadership to transfer", () -> {
      try {
        // wait until the address of the new leader matches the one we designated as the new leader
        return mCluster.getMasterAddresses()
                .get(mCluster.getPrimaryMasterIndex(MASTER_INDEX_WAIT_TIME)).equals(newLeaderAddr);
      } catch (Exception exc) {
        throw new RuntimeException(exc);
      }
    });

    mCluster.notifySuccess();
  }

  @Test
  public void repeatedTransferLeadership() throws Exception {
    for (int i = 0; i < NUM_MASTERS; i++) {
      int newLeaderIdx = (mCluster.getPrimaryMasterIndex(MASTER_INDEX_WAIT_TIME) + 1) % NUM_MASTERS;
      // `getPrimaryMasterIndex` uses the same `mMasterAddresses` variable as getMasterAddresses
      // we can therefore access to the new leader's address this way
      MasterNetAddress newLeaderAddr = mCluster.getMasterAddresses().get(newLeaderIdx);
      NetAddress netAddress = NetAddress.newBuilder().setHost(newLeaderAddr.getHostname())
              .setRpcPort(newLeaderAddr.getEmbeddedJournalPort()).build();

      mCluster.getJournalMasterClientForMaster().transferLeadership(netAddress);

      final int TIMEOUT_3MIN = 3 * 60 * 1000; // in ms
      CommonUtils.waitFor("leadership to transfer", () -> {
        try {
          // wait until the address of the new leader matches the one designated as the new leader
          return mCluster.getMasterAddresses()
              .get(mCluster.getPrimaryMasterIndex(MASTER_INDEX_WAIT_TIME)).equals(newLeaderAddr);
        } catch (Exception exc) {
          throw new RuntimeException(exc);
        }
      }, WaitForOptions.defaults().setTimeoutMs(TIMEOUT_3MIN));
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
      NetAddress netAddress = NetAddress.newBuilder().setHost(newLeaderAddr.getHostname())
              .setRpcPort(newLeaderAddr.getEmbeddedJournalPort()).build();

      mCluster.getJournalMasterClientForMaster().transferLeadership(netAddress);

      final int TIMEOUT_3MIN = 3 * 60 * 1000; // in ms
      CommonUtils.waitFor("leadership to transfer", () -> {
        try {
          // wait until the address of the new leader matches the one designated as the new leader
          return mCluster.getMasterAddresses()
              .get(mCluster.getPrimaryMasterIndex(MASTER_INDEX_WAIT_TIME)).equals(newLeaderAddr);
        } catch (Exception exc) {
          throw new RuntimeException(exc);
        }
      }, WaitForOptions.defaults().setTimeoutMs(TIMEOUT_3MIN));

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
}
