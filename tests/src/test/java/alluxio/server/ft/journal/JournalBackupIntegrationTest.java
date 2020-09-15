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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.RetryHandlingBlockMasterClient;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.client.meta.MetaMasterClient;
import alluxio.client.meta.RetryHandlingMetaMasterClient;
import alluxio.client.file.FileSystem;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.BackupAbortedException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.grpc.BackupPOptions;
import alluxio.grpc.BackupPRequest;
import alluxio.grpc.BackupState;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.WritePType;
import alluxio.master.MasterClientContext;
import alluxio.master.journal.JournalType;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.testutils.AlluxioOperationThread;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration test for backing up and restoring alluxio master.
 */
public final class JournalBackupIntegrationTest extends BaseIntegrationTest {
  public MultiProcessCluster mCluster;
  private static final int GET_PRIMARY_INDEX_TIMEOUT_MS = 30000;
  private static final int PRIMARY_KILL_TIMEOUT_MS = 30000;
  private static final int WAIT_NODES_REGISTERED_MS = 30000;

  @Rule
  public ConfigurationRule mConf = new ConfigurationRule(new HashMap<PropertyKey, String>() {
    {
      put(PropertyKey.USER_METRICS_COLLECTION_ENABLED, "false");
    }
  }, ServerConfiguration.global());

  @After
  public void after() throws Exception {
    if (mCluster != null) {
      mCluster.destroy();
    }
  }

  // This test needs to stop and start master many times, so it can take up to a minute to complete.
  @Test
  public void backupRestoreZk() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.BACKUP_RESTORE_ZK)
        .setClusterName("backupRestoreZk")
        .setNumMasters(3)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
        // Masters become primary faster
        .addProperty(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT, "3sec")
        .build();
    backupRestoreTest(true);
  }

  @Test
  public void backupRestoreMetastore_Heap() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.BACKUP_RESTORE_METASSTORE_HEAP)
        .setClusterName("backupRestoreMetastore_Heap")
        .setNumMasters(1)
        .setNumWorkers(1)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
        // Masters become primary faster
        .addProperty(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT, "1sec")
        .addProperty(PropertyKey.MASTER_METASTORE, "HEAP")
        .build();
    backupRestoreMetaStoreTest();
  }

  @Test
  public void backupRestoreMetastore_Rocks() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.BACKUP_RESTORE_METASSTORE_ROCKS)
        .setClusterName("backupRestoreMetastore_Rocks")
        .setNumMasters(1)
        .setNumWorkers(1)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
        // Masters become primary faster
        .addProperty(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT, "1sec")
        .addProperty(PropertyKey.MASTER_METASTORE, "ROCKS")
        .build();
    backupRestoreMetaStoreTest();
  }

  // This test needs to stop and start master many times, so it can take up to a minute to complete.
  @Test
  public void backupRestoreEmbedded() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.BACKUP_RESTORE_EMBEDDED)
        .setClusterName("backupRestoreEmbedded")
        .setNumMasters(3)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED.toString())
        .build();
    backupRestoreTest(true);
  }

  @Test
  public void backupRestoreSingleMaster() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.BACKUP_RESTORE_SINGLE)
        .setClusterName("backupRestoreSingle")
        .setNumMasters(1)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
        .build();
    backupRestoreTest(false);
  }

  // Tests various protocols and configurations for backup delegation.
  @Test
  public void backupDelegationProtocol() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.BACKUP_DELEGATION_PROTOCOL)
        .setClusterName("backupDelegationProtocol")
        .setNumMasters(3)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
        // Masters become primary faster
        .addProperty(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT, "1sec")
        // For faster backup role handshake.
        .addProperty(PropertyKey.MASTER_BACKUP_CONNECT_INTERVAL_MIN, "100ms")
        .addProperty(PropertyKey.MASTER_BACKUP_CONNECT_INTERVAL_MAX, "100ms")
        // Enable backup delegation
        .addProperty(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, "true")
        .build();

    File backups = AlluxioTestDirectory.createTemporaryDirectory("backups");
    mCluster.start();

    // Validate backup works with delegation.
    waitForBackup(BackupPRequest.newBuilder().setTargetDirectory(backups.getAbsolutePath())
        .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(false)).build());

    // Kill the primary.
    int primaryIdx = mCluster.getPrimaryMasterIndex(GET_PRIMARY_INDEX_TIMEOUT_MS);
    mCluster.waitForAndKillPrimaryMaster(PRIMARY_KILL_TIMEOUT_MS);

    // Validate backup works again after leader fail-over.
    waitForBackup(BackupPRequest.newBuilder().setTargetDirectory(backups.getAbsolutePath())
        .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(false)).build());

    // Continue testing with 2 masters...
    // Find standby master index.
    int newPrimaryIdx = mCluster.getPrimaryMasterIndex(GET_PRIMARY_INDEX_TIMEOUT_MS);
    int followerIdx = (newPrimaryIdx + 1) % 2;
    if (followerIdx == primaryIdx) {
      followerIdx = (followerIdx + 1) % 2;
    }

    // Kill the follower. (only leader remains).
    mCluster.stopMaster(followerIdx);

    // Wait for a second for process to terminate properly.
    // This is so that backup request don't get delegated to follower before termination.
    Thread.sleep(1000);

    // Validate backup delegation fails.
    try {
      mCluster.getMetaMasterClient()
          .backup(BackupPRequest.newBuilder().setTargetDirectory(backups.getAbsolutePath())
              .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(false)).build());
      Assert.fail("Cannot delegate backup with no followers.");
    } catch (FailedPreconditionException e) {
      // Expected to fail since there is only single master.
    }

    // Should work with "AllowLeader" backup.
    mCluster.getMetaMasterClient()
        .backup(BackupPRequest.newBuilder().setTargetDirectory(backups.getAbsolutePath())
            .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(false).setAllowLeader(true))
            .build());

    // Restart the follower. (1 leader 1 follower remains).
    mCluster.startMaster(followerIdx);

    // Validate backup works again.
    waitForBackup(BackupPRequest.newBuilder().setTargetDirectory(backups.getAbsolutePath())
        .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(false)).build());

    // Schedule async backup.
    UUID backupId = mCluster.getMetaMasterClient()
        .backup(BackupPRequest.newBuilder().setTargetDirectory(backups.getAbsolutePath())
            .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(false).setRunAsync(true))
            .build())
        .getBackupId();
    // Wait until backup is complete.
    CommonUtils.waitFor("Backup completed.", () -> {
      try {
        return mCluster.getMetaMasterClient().getBackupStatus(backupId).getState()
            .equals(BackupState.Completed);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Unexpected error while getting backup status: %s", e.toString()));
      }
    });

    // Schedule a local backup to overwrite latest backup Id in the current leader.
    mCluster.getMetaMasterClient()
        .backup(BackupPRequest.newBuilder().setTargetDirectory(backups.getAbsolutePath())
            .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(false).setAllowLeader(true))
            .build());

    // Validate old backup can still be queried.
    mCluster.getMetaMasterClient().getBackupStatus(backupId);

    mCluster.notifySuccess();
  }

  // Tests various protocols and configurations for backup delegation during fail-overs.
  @Test
  public void backupDelegationFailoverProtocol() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.BACKUP_DELEGATION_FAILOVER_PROTOCOL)
        .setClusterName("backupDelegationFailoverProtocol")
        .setNumMasters(2)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
        // Masters become primary faster
        .addProperty(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT, "1sec")
        // For faster backup role handshake.
        .addProperty(PropertyKey.MASTER_BACKUP_CONNECT_INTERVAL_MIN, "100ms")
        .addProperty(PropertyKey.MASTER_BACKUP_CONNECT_INTERVAL_MAX, "100ms")
        // Enable backup delegation
        .addProperty(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, "true")
        // Set backup timeout to be shorter
        .addProperty(PropertyKey.MASTER_BACKUP_ABANDON_TIMEOUT, "3sec")
        .build();

    File backups = AlluxioTestDirectory.createTemporaryDirectory("backups");
    mCluster.start();

    // Validate backup works with delegation.
    waitForBackup(BackupPRequest.newBuilder().setTargetDirectory(backups.getAbsolutePath())
        .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(false)).build());

    // Find standby master index.
    int primaryIdx = mCluster.getPrimaryMasterIndex(GET_PRIMARY_INDEX_TIMEOUT_MS);
    int followerIdx = (primaryIdx + 1) % 2;

    // Schedule async backup.
    UUID backupId = mCluster.getMetaMasterClient()
        .backup(BackupPRequest.newBuilder().setTargetDirectory(backups.getAbsolutePath())
            .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(false).setRunAsync(true))
            .build())
        .getBackupId();
    // Kill follower immediately before it sends the next heartbeat to leader.
    mCluster.stopMaster(followerIdx);
    // Wait until backup is abandoned.
    CommonUtils.waitForResult("Backup abandoned.", () -> {
      try {
        return mCluster.getMetaMasterClient().getBackupStatus(backupId);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Unexpected error while getting backup status: %s", e.toString()));
      }
    }, (backupStatus) -> backupStatus.getError() instanceof BackupAbortedException);

    // Restart follower to restore HA.
    mCluster.startMaster(followerIdx);

    // Validate delegated backup works again.
    waitForBackup(BackupPRequest.newBuilder().setTargetDirectory(backups.getAbsolutePath())
        .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(false)).build());

    // Schedule async backup.
    mCluster.getMetaMasterClient()
        .backup(BackupPRequest.newBuilder().setTargetDirectory(backups.getAbsolutePath())
            .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(false).setRunAsync(true))
            .build())
        .getBackupId();

    // Kill leader immediately before it receives the next heartbeat from backup-worker.
    mCluster.waitForAndKillPrimaryMaster(PRIMARY_KILL_TIMEOUT_MS);
    // Wait until follower steps up.
    assertEquals(mCluster.getPrimaryMasterIndex(GET_PRIMARY_INDEX_TIMEOUT_MS), followerIdx);

    // Follower should step-up without problem and accept backup requests.
    mCluster.getMetaMasterClient()
        .backup(BackupPRequest.newBuilder().setTargetDirectory(backups.getAbsolutePath())
            .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(false).setAllowLeader(true))
            .build());

    mCluster.notifySuccess();
  }

  @Test
  public void backupDelegationZk() throws Exception {
    MultiProcessCluster.Builder clusterBuilder = MultiProcessCluster
        .newBuilder(PortCoordination.BACKUP_DELEGATION_ZK)
        .setClusterName("backupDelegationZk")
        .setNumMasters(2)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
        // Masters become primary faster
        .addProperty(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT, "1sec")
        // Enable backup delegation
        .addProperty(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, "true")
        // For faster backup role handshake.
        .addProperty(PropertyKey.MASTER_BACKUP_CONNECT_INTERVAL_MIN, "100ms")
        .addProperty(PropertyKey.MASTER_BACKUP_CONNECT_INTERVAL_MAX, "100ms");
    backupDelegationTest(clusterBuilder);
  }

  @Test
  public void backupDelegationEmbedded() throws Exception {
    MultiProcessCluster.Builder clusterBuilder =
        MultiProcessCluster.newBuilder(PortCoordination.BACKUP_DELEGATION_EMBEDDED)
            .setClusterName("backupDelegationEmbedded")
            .setNumMasters(2)
            .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED.toString())
            // Enable backup delegation
            .addProperty(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, "true")
            // For faster backup role handshake.
            .addProperty(PropertyKey.MASTER_BACKUP_CONNECT_INTERVAL_MIN, "100ms")
            .addProperty(PropertyKey.MASTER_BACKUP_CONNECT_INTERVAL_MAX, "100ms");
    backupDelegationTest(clusterBuilder);
  }

  /**
   * Used to wait until successful backup.
   *
   * It tolerates {@link FailedPreconditionException} that could be thrown when backup-workers have
   * not established connection with the backup-leader yet.
   *
   * @param backupRequest the backup request
   * @return backup uri
   */
  private AlluxioURI waitForBackup(BackupPRequest backupRequest) throws Exception {
    AtomicReference<AlluxioURI> backupUriRef = new AtomicReference<>(null);
    CommonUtils.waitFor("Backup delegation to succeed.", () -> {
      try {
        backupUriRef.set(mCluster.getMetaMasterClient().backup(backupRequest).getBackupUri());
        return true;
      } catch (FailedPreconditionException e1) {
        // Expected to fail with this until backup-workers connect with backup-leader.
        return false;
      } catch (Exception e2) {
        throw new RuntimeException(
            String.format("Backup failed with unexpected error: %s", e2.toString()));
      }
    }, WaitForOptions.defaults());
    return backupUriRef.get();
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
          CreateDirectoryPOptions.newBuilder().setWriteType(WritePType.MUST_CACHE).build());
      AlluxioURI backup1 = metaClient
          .backup(BackupPRequest.newBuilder().setTargetDirectory(backups.getAbsolutePath())
              .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(false)).build())
          .getBackupUri();
      AlluxioURI dir2 = new AlluxioURI("/dir2");
      fs.createDirectory(dir2,
          CreateDirectoryPOptions.newBuilder().setWriteType(WritePType.MUST_CACHE).build());
      AlluxioURI backup2 = metaClient
          .backup(BackupPRequest.newBuilder().setTargetDirectory(backups.getAbsolutePath())
              .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(false)).build())
          .getBackupUri();

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
      opThreads.forEach(Thread::interrupt);
    }
  }

  private void backupDelegationTest(MultiProcessCluster.Builder clusterBuilder) throws Exception {
    // Update configuration for each master to backup to a specific folder.
    Map<Integer, Map<PropertyKey, String>> masterProps = new HashMap<>();
    File backupsParent0 = AlluxioTestDirectory.createTemporaryDirectory("backups0");
    File backupsParent1 = AlluxioTestDirectory.createTemporaryDirectory("backups1");
    Map<PropertyKey, String> master0Props = new HashMap<>();
    master0Props.put(PropertyKey.MASTER_BACKUP_DIRECTORY, backupsParent0.getAbsolutePath());
    Map<PropertyKey, String> master1Props = new HashMap<>();
    master1Props.put(PropertyKey.MASTER_BACKUP_DIRECTORY, backupsParent1.getAbsolutePath());
    masterProps.put(0, master0Props);
    masterProps.put(1, master1Props);
    clusterBuilder.setMasterProperties(masterProps);
    // Start cluster.
    mCluster = clusterBuilder.build();
    mCluster.start();

    // Delegation test can work with 2 masters only.
    assertEquals(2, mCluster.getMasterAddresses().size());

    FileSystem fs = mCluster.getFileSystemClient();

    AlluxioURI dir1 = new AlluxioURI("/dir1");
    mCluster.getFileSystemClient().createDirectory(dir1,
        CreateDirectoryPOptions.newBuilder().setWriteType(WritePType.MUST_CACHE).build());

    AlluxioURI backupUri = waitForBackup(BackupPRequest.newBuilder()
        .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(true)).build());

    int primaryIndex = mCluster.getPrimaryMasterIndex(GET_PRIMARY_INDEX_TIMEOUT_MS);
    int followerIndex = (primaryIndex + 1) % 2;

    // Validate backup is taken on follower's local path.
    assertTrue(backupUri.toString()
        .contains(masterProps.get(followerIndex).get(PropertyKey.MASTER_BACKUP_DIRECTORY)));

    // Validate backup is valid.
    restartMastersFromBackup(backupUri);
    assertTrue(fs.exists(dir1));

    mCluster.notifySuccess();
  }

  private void backupRestoreMetaStoreTest() throws Exception {
    // Directory for backups.
    File backups = AlluxioTestDirectory.createTemporaryDirectory("backups");
    mCluster.start();

    // Needs workers to be up before the test.
    mCluster.waitForAllNodesRegistered(WAIT_NODES_REGISTERED_MS);

    // Acquire clients.
    FileSystem fs = mCluster.getFileSystemClient();
    MetaMasterClient metaClient = getMetaClient(mCluster);
    BlockMasterClient blockClient = getBlockClient(mCluster);

    // Create single test file.
    String testFilePath = "/file";
    AlluxioURI testFileUri = new AlluxioURI(testFilePath);

    // Create file THROUGH.
    FileSystemTestUtils.createByteFile(fs, testFilePath, 100,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.THROUGH).build());
    // Delete it from Alluxio namespace.
    fs.delete(testFileUri, DeletePOptions.newBuilder().setAlluxioOnly(true).build());
    // List status on root to bring the file's meta back.
    fs.listStatus(new AlluxioURI("/"), ListStatusPOptions.newBuilder().setRecursive(true)
        .setLoadMetadataType(LoadMetadataPType.ONCE).build());
    // Verify that file's meta is in Alluxio.
    assertNotNull(fs.getStatus(testFileUri));

    // Take a backup.
    AlluxioURI backup1 = metaClient
        .backup(BackupPRequest.newBuilder().setTargetDirectory(backups.getAbsolutePath())
            .setOptions(BackupPOptions.newBuilder().setLocalFileSystem(false)).build())
        .getBackupUri();
    // Restart with backup.
    restartMastersFromBackup(backup1);

    // Verify that file and its blocks are in Alluxio after restore.
    URIStatus fileStatus = fs.getStatus(testFileUri);
    assertNotNull(fileStatus);
    for (long blockId : fileStatus.getBlockIds()) {
      assertNotNull(blockClient.getBlockInfo(blockId));
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
        MasterClientContext.newBuilder(ClientContext.create(ServerConfiguration.global()))
            .setMasterInquireClient(cluster.getMasterInquireClient())
            .build());
  }

  private BlockMasterClient getBlockClient(MultiProcessCluster cluster) {
    return new RetryHandlingBlockMasterClient(
        MasterClientContext.newBuilder(ClientContext.create(ServerConfiguration.global()))
            .setMasterInquireClient(cluster.getMasterInquireClient()).build());
  }
}
