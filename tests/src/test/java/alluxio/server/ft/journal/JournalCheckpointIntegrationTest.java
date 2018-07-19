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
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.MetaMasterClient;
import alluxio.client.RetryHandlingMetaMasterClient;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.MasterClientConfig;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Tests checkpoints remain consistent with intended master state.
 */
public class JournalCheckpointIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, 0)
          .setNumWorkers(0)
          .build();

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Test
  public void recoverMounts() throws Exception {
    LocalAlluxioCluster mCluster = mClusterResource.get();
    AlluxioURI alluxioMount1 = new AlluxioURI("/mount1");
    AlluxioURI alluxioMount2 = new AlluxioURI("/mount2");
    AlluxioURI fileMount1 = new AlluxioURI(mFolder.newFolder("1").getAbsolutePath());
    AlluxioURI fileMount2 = new AlluxioURI(mFolder.newFolder("2").getAbsolutePath());
    mCluster.getClient().mount(alluxioMount1, fileMount1);
    mCluster.getClient().mount(alluxioMount2, fileMount2);

    // Take a backup and restart.
    File backup = mFolder.newFolder("backup");
    MetaMasterClient metaClient = new RetryHandlingMetaMasterClient(MasterClientConfig.defaults());
    AlluxioURI backupURI = metaClient.backup(backup.getAbsolutePath(), true).getBackupUri();
    Configuration.set(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP, backupURI);
    mCluster.formatAndRestartMasters();

    assertEquals(3, mCluster.getClient().getMountTable().size());
    mCluster.getClient().unmount(alluxioMount1);
    assertEquals(2, mCluster.getClient().getMountTable().size());
    Configuration.unset(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP);
  }
}
