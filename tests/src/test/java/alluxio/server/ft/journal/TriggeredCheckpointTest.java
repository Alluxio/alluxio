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
import alluxio.client.meta.MetaMasterClient;
import alluxio.conf.PropertyKey;
import alluxio.master.NoopMaster;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalSnapshot;
import alluxio.metrics.MasterMetrics;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.util.URIUtils;

import org.junit.Assert;
import org.junit.Test;

import java.net.URI;

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
    cluster.start();
    try {
      cluster.waitForAllNodesRegistered(20 * Constants.SECOND_MS);
      String journalLocation = cluster.getJournalDir();
      UfsJournal ufsJournal = new UfsJournal(URIUtils.appendPathOrDie(new URI(journalLocation),
          Constants.FILE_SYSTEM_MASTER_NAME), new NoopMaster(""), 0);

      // Creates journal entries
      FileSystem fs = cluster.getFileSystemClient();
      for (int i = 0; i < numFiles; i++) {
        fs.createFile(new AlluxioURI("/file" + i)).close();
      }
      MetaMasterClient meta = cluster.getMetaMasterClient();
      assertEquals(numFiles + 1,
          meta.getMetrics().get("Master." + MasterMetrics.TOTAL_PATHS).getLongValue());

      // Triggers checkpoint
      Assert.assertEquals(cluster.getMasterAddresses().get(0).getHostname(), meta.checkpoint());
      Assert.assertEquals(1, UfsJournalSnapshot.getSnapshot(ufsJournal).getCheckpoints().size());

      // Restart masters to validate the created checkpoint is valid
      cluster.stopMasters();
      cluster.startMasters();
      cluster.waitForAllNodesRegistered(20 * Constants.SECOND_MS);
      fs = cluster.getFileSystemClient();
      assertEquals(100, fs.listStatus(new AlluxioURI("/")).size());
      meta = cluster.getMetaMasterClient();
      assertEquals(101,
          meta.getMetrics().get("Master." + MasterMetrics.TOTAL_PATHS).getLongValue());
      cluster.notifySuccess();
    } finally {
      cluster.destroy();
    }
  }

  @Test
  public void embeddedJournal() throws Exception {
    int numFiles = 100;
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

      // Creates journal entries
      FileSystem fs = cluster.getFileSystemClient();
      for (int i = 0; i < numFiles; i++) {
        fs.createFile(new AlluxioURI("/file" + i)).close();
      }
      MetaMasterClient meta = cluster.getMetaMasterClient();
      assertEquals(numFiles + 1,
          meta.getMetrics().get("Master." + MasterMetrics.TOTAL_PATHS).getLongValue());

      // Triggers checkpoint and check if checkpoint exists
      Assert.assertEquals(cluster.getMasterAddresses().get(0).getHostname(), meta.checkpoint());

      // Restart masters to validate the created checkpoint is valid
      cluster.stopMasters();
      cluster.startMasters();
      cluster.waitForAllNodesRegistered(20 * Constants.SECOND_MS);
      fs = cluster.getFileSystemClient();
      assertEquals(100, fs.listStatus(new AlluxioURI("/")).size());
      meta = cluster.getMetaMasterClient();
      assertEquals(101,
          meta.getMetrics().get("Master." + MasterMetrics.TOTAL_PATHS).getLongValue());
      cluster.notifySuccess();
    } finally {
      cluster.destroy();
    }
  }
}
