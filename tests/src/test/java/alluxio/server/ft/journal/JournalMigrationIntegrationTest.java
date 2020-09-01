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

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.conf.PropertyKey;
import alluxio.client.meta.MetaMasterClient;
import alluxio.client.meta.RetryHandlingMetaMasterClient;
import alluxio.client.file.FileSystem;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.BackupPOptions;
import alluxio.grpc.BackupPRequest;
import alluxio.master.MasterClientContext;
import alluxio.master.journal.JournalType;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.MultiProcessCluster.DeployMode;
import alluxio.multi.process.PortCoordination;
import alluxio.testutils.BaseIntegrationTest;

import org.junit.Test;

import java.io.File;

/**
 * Integration test for migrating between UFS and embedded journals.
 */
public final class JournalMigrationIntegrationTest extends BaseIntegrationTest {
  private static final int NUM_DIRS = 10;

  @Test
  public void migration() throws Exception {
    MultiProcessCluster cluster = MultiProcessCluster.newBuilder(PortCoordination.JOURNAL_MIGRATION)
        .setClusterName("journalMigration")
        .setNumMasters(3)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
        // Masters become primary faster
        .addProperty(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT, "1sec")
        .build();
    try {
      cluster.start();
      FileSystem fs = cluster.getFileSystemClient();
      MetaMasterClient metaClient = new RetryHandlingMetaMasterClient(
          MasterClientContext.newBuilder(ClientContext.create(ServerConfiguration.global()))
              .setMasterInquireClient(cluster.getMasterInquireClient())
              .build());
      for (int i = 0; i < NUM_DIRS; i++) {
        fs.createDirectory(new AlluxioURI("/dir" + i));
      }
      File backupsDir = AlluxioTestDirectory.createTemporaryDirectory("backups");
      AlluxioURI zkBackup = metaClient
          .backup(BackupPRequest.newBuilder().setTargetDirectory(backupsDir.getAbsolutePath())
              .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(false)).build())
          .getBackupUri();
      cluster.updateMasterConf(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP, zkBackup.toString());

      // Migrate to embedded journal HA.
      cluster.stopMasters();
      cluster.formatJournal();
      cluster.updateDeployMode(DeployMode.EMBEDDED);
      cluster.startMasters();
      assertEquals(NUM_DIRS, fs.listStatus(new AlluxioURI("/")).size());

      // Migrate back to Zookeeper HA.
      cluster.stopMasters();
      cluster.formatJournal();
      cluster.updateDeployMode(DeployMode.ZOOKEEPER_HA);
      cluster.startMasters();
      assertEquals(NUM_DIRS, fs.listStatus(new AlluxioURI("/")).size());

      cluster.notifySuccess();
    } finally {
      cluster.destroy();
    }
  }
}
