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

package alluxio.server.ft.journal;

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.MetaMasterClient;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MasterMetrics;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.MultiProcessCluster.DeployMode;
import alluxio.multi.process.PortCoordination;
import alluxio.testutils.IntegrationTestUtils;

import org.junit.Test;

import java.net.URI;

public class MultiProcessCheckpointTest {

  @Test
  public void test() throws Exception {
    MultiProcessCluster cluster = MultiProcessCluster.newBuilder(PortCoordination.CHECKPOINT)
        .setClusterName("CheckpointTest")
        .setDeployMode(DeployMode.ZOOKEEPER_HA)
        .addProperty(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT, "2sec")
        .addProperty(PropertyKey.ZOOKEEPER_CONNECTION_TIMEOUT, "1sec")
        .addProperty(PropertyKey.MASTER_METASTORE, "ROCKS")
        .addProperty(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, "100")
        .addProperty(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, "500")
        .addProperty(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, "500")
        .setNumMasters(2)
        .setNumWorkers(0)
        .build();
    cluster.start();
    try {
      cluster.waitForAllNodesRegistered(20 * Constants.SECOND_MS);
      String journal = cluster.getJournalDir();
      FileSystem fs = cluster.getFileSystemClient();
      int numFiles = 100;
      for (int i = 0; i < numFiles; i++) {
        fs.createFile(new AlluxioURI("/file" + i)).close();
      }
      MetaMasterClient meta = cluster.getMetaMasterClient();
      assertEquals(numFiles + 1,
          meta.getMetrics().get("Master." + MasterMetrics.TOTAL_PATHS).getLongValue());
      IntegrationTestUtils.waitForUfsJournalCheckpoint(Constants.FILE_SYSTEM_MASTER_NAME,
          new URI(journal));
      cluster.stopMasters();
      cluster.startMasters();
      cluster.waitForAllNodesRegistered(20 * Constants.SECOND_MS);
      fs = cluster.getFileSystemClient();
      assertEquals(numFiles, fs.listStatus(new AlluxioURI("/")).size());
      meta = cluster.getMetaMasterClient();
      assertEquals(numFiles + 1,
          meta.getMetrics().get("Master." + MasterMetrics.TOTAL_PATHS).getLongValue());
      cluster.notifySuccess();
    } finally {
      cluster.destroy();
    }
  }
}
