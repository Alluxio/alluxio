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

import alluxio.ConfigurationRule;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.NetAddress;
import alluxio.grpc.QuorumServerInfo;
import alluxio.multi.process.MasterNetAddress;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.junit.After;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

public class EmbeddedJournalIntegrationTestBase extends BaseIntegrationTest {
  private static final Logger LOG = LoggerFactory
      .getLogger(EmbeddedJournalIntegrationTestBase.class);

  @Rule
  public ConfigurationRule mConf =
      new ConfigurationRule(PropertyKey.USER_METRICS_COLLECTION_ENABLED, false,
          Configuration.modifiableGlobal());

  public MultiProcessCluster mCluster;

  protected static final int MASTER_INDEX_WAIT_TIME = 5_000; // milliseconds

  @After
  public void after() throws Exception {
    if (mCluster != null) {
      mCluster.destroy();
    }
  }

  protected NetAddress masterEBJAddr2NetAddr(MasterNetAddress masterAddr) {
    return NetAddress.newBuilder().setHost(masterAddr.getHostname())
        .setRpcPort(masterAddr.getEmbeddedJournalPort()).build();
  }

  protected void waitForQuorumPropertySize(Predicate<? super QuorumServerInfo> pred, int size)
      throws InterruptedException, TimeoutException {
    final int TIMEOUT_1MIN30SEC = 90 * 1000; // in ms
    Configuration.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "10sec");
    try {
      CommonUtils.waitFor("quorum property", () -> {
        try {
          return mCluster.getJournalMasterClientForMaster().getQuorumInfo().getServerInfoList()
              .stream().filter(pred).count() == size;
        } catch (AlluxioStatusException e) {
          return false;
        }
      }, WaitForOptions.defaults().setTimeoutMs(TIMEOUT_1MIN30SEC).setInterval(200));
    } catch (Exception e) {
      LOG.error("Error in wait for", e);
      throw e;
    }
  }
}