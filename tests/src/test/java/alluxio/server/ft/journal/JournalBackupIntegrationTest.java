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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.MetaMasterClient;
import alluxio.client.RetryHandlingMetaMasterClient;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.master.MasterClientConfig;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.MultiProcessCluster.DeployMode;
import alluxio.testutils.AlluxioOperationThread;
import alluxio.testutils.BaseIntegrationTest;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Integration test for backing up and restoring alluxio master.
 */
public final class JournalBackupIntegrationTest extends BaseIntegrationTest {
  public MultiProcessCluster mCluster;

  @Rule
  public ConfigurationRule mConf = new ConfigurationRule(new HashMap<PropertyKey, String>() {
    {
      put(PropertyKey.USER_METRICS_COLLECTION_ENABLED, "false");
    }
  });

  @After
  public void after() throws Exception {
    if (mCluster != null) {
      mCluster.destroy();
    }
  }

  // This test needs to stop and start master many times, so it can take up to a minute to complete.
  @Test
  public void backupRestoreZk() throws Exception {
    mCluster = MultiProcessCluster.newBuilder()
        .setClusterName("backupRestoreZk")
        .setDeployMode(DeployMode.ZOOKEEPER_HA)
        .setNumMasters(3)
        // Masters become primary faster
        .addProperty(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT, "1sec").build();
    backupRestoreTest(true);
  }

  @Test
  public void backupRestoreSingleMaster() throws Exception {
    mCluster = MultiProcessCluster.newBuilder()
        .setClusterName("backupRestoreSingle")
        .setNumMasters(1).build();
    backupRestoreTest(false);
  }

  private void backupRestoreTest(boolean testFailover) throws Exception {
    File backups = AlluxioTestDirectory.createTemporaryDirectory("backups");
    mCluster.start();
    List<Thread> opThreads = new ArrayList<>();
    // Run background threads to perform metadata operations while the journal backups and restores
    // are happening.
    for (int i = 0; i < 10; i++) {
      AlluxioOperationThread thread = new AlluxioOperationThread(mCluster.getFileSystemClient());
      thread.start();
      opThreads.add(thread);
    }
    try {
      FileSystem fs = mCluster.getFileSystemClient();
      MetaMasterClient metaClient = getMetaClient(mCluster);

      AlluxioURI dir1 = new AlluxioURI("/dir1");
      fs.createDirectory(dir1,
          CreateDirectoryOptions.defaults().setWriteType(WriteType.MUST_CACHE));
      AlluxioURI backup1 =
          metaClient.backup(backups.getAbsolutePath(), false).getBackupUri();
      AlluxioURI dir2 = new AlluxioURI("/dir2");
      fs.createDirectory(dir2,
          CreateDirectoryOptions.defaults().setWriteType(WriteType.MUST_CACHE));
      AlluxioURI backup2 =
          metaClient.backup(backups.getAbsolutePath(), false).getBackupUri();

      restartMastersFromBackup(backup2);
      assertTrue(fs.exists(dir1));
      assertTrue(fs.exists(dir2));

      restartMastersFromBackup(backup1);
      assertTrue(fs.exists(dir1));
      assertFalse(fs.exists(dir2));

      // Restart normally and make sure we remember the state from backup 1.
      mCluster.stopMasters();
      mCluster.startMasters();
      assertTrue(fs.exists(dir1));
      assertFalse(fs.exists(dir2));

      if (testFailover) {
        // Verify that failover works correctly.
        mCluster.waitForAndKillPrimaryMaster(30 * Constants.SECOND_MS);
        assertTrue(fs.exists(dir1));
        assertFalse(fs.exists(dir2));
      }

      mCluster.notifySuccess();
    } finally {
      opThreads.forEach(thread -> thread.interrupt());
    }
  }

  private void restartMastersFromBackup(AlluxioURI backup) throws IOException {
    mCluster.stopMasters();
    mCluster.formatJournal();
    mCluster.updateMasterConf(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP, backup.toString());
    mCluster.startMasters();
    mCluster.updateMasterConf(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP, null);
  }

  private MetaMasterClient getMetaClient(MultiProcessCluster cluster) {
    return new RetryHandlingMetaMasterClient(
        MasterClientConfig.defaults().withMasterInquireClient(cluster.getMasterInquireClient()));
  }
}
