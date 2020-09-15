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
import alluxio.client.file.FileSystem;
import alluxio.client.metrics.MetricsMasterClient;
import alluxio.conf.PropertyKey;
import alluxio.master.NoopMaster;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalSnapshot;
import alluxio.metrics.MetricKey;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.util.URIUtils;

import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.Collections;

public class TriggeredCheckpointTest {
  @Test
  public void ufsJournal() throws Exception {
    int numFiles = 100;
    MultiProcessCluster cluster = MultiProcessCluster
        .newBuilder(PortCoordination.TRIGGERED_UFS_CHECKPOINT)
        .setClusterName("TriggeredUfsCheckpointTest")
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
        .addProperty(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, String.valueOf(numFiles))
        .setNumMasters(1)
        .setNumWorkers(1)
        .build();
    try {
      cluster.start();
      cluster.waitForAllNodesRegistered(20 * Constants.SECOND_MS);

      // Get enough journal entries
      createFiles(cluster, numFiles);

      // Trigger checkpoint
      Assert.assertEquals(cluster.getMasterAddresses().get(0).getHostname(),
          cluster.getMetaMasterClient().checkpoint());
      String journalLocation = cluster.getJournalDir();
      UfsJournal ufsJournal = new UfsJournal(URIUtils.appendPathOrDie(new URI(journalLocation),
          Constants.FILE_SYSTEM_MASTER_NAME), new NoopMaster(""), 0, Collections::emptySet);
      Assert.assertEquals(1, UfsJournalSnapshot.getSnapshot(ufsJournal).getCheckpoints().size());

      validateCheckpointInClusterRestart(cluster);
      cluster.notifySuccess();
    } finally {
      cluster.destroy();
    }
  }

  @Test
  public void embeddedJournal() throws Exception {
    MultiProcessCluster cluster = MultiProcessCluster
        .newBuilder(PortCoordination.TRIGGERED_EMBEDDED_CHECKPOINT)
        .setClusterName("TriggeredEmbeddedCheckpointTest")
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED.toString())
        .addProperty(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, String.valueOf(Constants.KB))
        .setNumMasters(1)
        .setNumWorkers(1)
        .build();
    cluster.start();
    try {
      cluster.waitForAllNodesRegistered(20 * Constants.SECOND_MS);

      // Get enough journal entries
      createFiles(cluster, 100);

      // Trigger checkpoint and check if checkpoint exists
      Assert.assertEquals(cluster.getMasterAddresses().get(0).getHostname(),
          cluster.getMetaMasterClient().checkpoint());

      validateCheckpointInClusterRestart(cluster);
      cluster.notifySuccess();
    } finally {
      cluster.destroy();
    }
  }

  /**
   * Creates files in the cluster.
   *
   * @param cluster the cluster inside which to create files
   * @param numFiles num of files to create
   */
  private void createFiles(MultiProcessCluster cluster, int numFiles)
      throws Exception {
    FileSystem fs = cluster.getFileSystemClient();
    for (int i = 0; i < numFiles; i++) {
      fs.createFile(new AlluxioURI("/file" + i)).close();
    }
    MetricsMasterClient metricsMasterClient = cluster.getMetricsMasterClient();
    assertEquals(numFiles + 1, (long) metricsMasterClient
        .getMetrics().get(MetricKey.MASTER_TOTAL_PATHS.getName()).getDoubleValue());
  }

  /**
   * Validates checkpoint by restarting the cluster.
   *
   * @param cluster the cluster to restart
   */
  private void validateCheckpointInClusterRestart(MultiProcessCluster cluster)
      throws Exception {
    cluster.stopMasters();
    cluster.startMasters();
    cluster.waitForAllNodesRegistered(40 * Constants.SECOND_MS);
    assertEquals(100, cluster.getFileSystemClient().listStatus(new AlluxioURI("/")).size());
    assertEquals(101, (long) cluster.getMetricsMasterClient().getMetrics()
        .get(MetricKey.MASTER_TOTAL_PATHS.getName()).getDoubleValue());
  }
}
