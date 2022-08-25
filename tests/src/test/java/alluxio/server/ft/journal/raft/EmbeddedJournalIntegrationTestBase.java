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
import alluxio.master.journal.JournalType;
import alluxio.multi.process.MasterNetAddress;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Rule;

import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

public class EmbeddedJournalIntegrationTestBase extends BaseIntegrationTest {

  Map<PropertyKey, Object> mDefaultProperties = ImmutableMap.<PropertyKey, Object>builder()
      .put(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED)
      .put(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
      .put(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "750ms")
      .put(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1500ms")
      .put(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE, 1)
      .put(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE, 1)
      .put(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE, 100)
      .build();

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
    CommonUtils.waitFor("quorum property", () -> {
      try {
        return mCluster.getJournalMasterClientForMaster().getQuorumInfo().getServerInfoList()
            .stream().filter(pred).count() == size;
      } catch (AlluxioStatusException e) {
        return false;
      }
    }, WaitForOptions.defaults().setTimeoutMs(TIMEOUT_1MIN30SEC).setInterval(200));
  }
}
