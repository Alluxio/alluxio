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
import static org.junit.Assert.fail;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.client.meta.MetaMasterClient;
import alluxio.client.meta.RetryHandlingMetaMasterClient;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.RetryHandlingFileSystemMasterClient;
import alluxio.exception.AccessControlException;
import alluxio.grpc.BackupPOptions;
import alluxio.grpc.BackupPRequest;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.UfsPMode;
import alluxio.grpc.UpdateUfsModePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.MasterClientContext;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Tests checkpoints remain consistent with intended master state.
 */
public class JournalCheckpointIntegrationTest extends BaseIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, 0)
          .setNumWorkers(0)
          .build();

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  public LocalAlluxioCluster mCluster;

  @Before
  public void before() {
    mCluster = mClusterResource.get();
  }

  @Test
  public void recoverMounts() throws Exception {
    AlluxioURI alluxioMount1 = new AlluxioURI("/mount1");
    AlluxioURI alluxioMount2 = new AlluxioURI("/mount2");
    AlluxioURI fileMount1 = new AlluxioURI(mFolder.newFolder("1").getAbsolutePath());
    AlluxioURI fileMount2 = new AlluxioURI(mFolder.newFolder("2").getAbsolutePath());
    mCluster.getClient().mount(alluxioMount1, fileMount1);
    mCluster.getClient().mount(alluxioMount2, fileMount2);

    backupAndRestore();

    assertEquals(3, mCluster.getClient().getMountTable().size());
    mCluster.getClient().unmount(alluxioMount1);
    assertEquals(2, mCluster.getClient().getMountTable().size());
    ServerConfiguration.unset(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP);
  }

  @Test
  public void recoverUfsState() throws Exception {
    FileSystemMasterClient client =
        new RetryHandlingFileSystemMasterClient(MasterClientContext
            .newBuilder(ClientContext.create(ServerConfiguration.global())).build());
    client.updateUfsMode(new AlluxioURI(""),
        UpdateUfsModePOptions.newBuilder().setUfsMode(UfsPMode.READ_ONLY).build());

    backupAndRestore();
    try {
      mCluster.getClient().createDirectory(new AlluxioURI("/test"),
          CreateDirectoryPOptions.newBuilder().setWriteType(WritePType.THROUGH).build());
      fail("Expected an exception to be thrown");
    } catch (AccessControlException e) {
      // Expected
    }
  }

  private void backupAndRestore() throws Exception {
    File backup = mFolder.newFolder("backup");
    MetaMasterClient metaClient =
        new RetryHandlingMetaMasterClient(MasterClientContext
            .newBuilder(ClientContext.create(ServerConfiguration.global())).build());
    AlluxioURI backupURI = metaClient
        .backup(BackupPRequest.newBuilder().setTargetDirectory(backup.getAbsolutePath())
            .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(true)).build())
        .getBackupUri();
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP, backupURI);
    mCluster.formatAndRestartMasters();
  }
}
