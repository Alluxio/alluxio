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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.AlluxioMasterProcess;
import alluxio.master.journal.JournalType;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.testutils.BaseIntegrationTest;

import org.junit.After;
import org.junit.Rule;

import java.util.UUID;

public class BaseEmbeddedJournalTest extends BaseIntegrationTest {

  protected static final int NUM_MASTERS = 5;

  @Rule
  public ConfigurationRule mConf =
          new ConfigurationRule(PropertyKey.USER_METRICS_COLLECTION_ENABLED, "false",
                  ServerConfiguration.global());

  public MultiProcessCluster mCluster;
  // Used by growCluster test.
  protected AlluxioMasterProcess mNewMaster;

  protected void standardBefore() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_FAILOVER)
            .setClusterName(UUID.randomUUID().toString())
            .setNumMasters(NUM_MASTERS)
            .setNumWorkers(0)
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
    if (mNewMaster != null) {
      mNewMaster.stop();
      mNewMaster = null;
    }
  }
}
