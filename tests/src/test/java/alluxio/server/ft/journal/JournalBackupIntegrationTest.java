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
import alluxio.client.WriteType;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.RetryHandlingBlockMasterClient;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.client.meta.MetaMasterClient;
import alluxio.client.meta.RetryHandlingMetaMasterClient;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
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
import alluxio.master.NoopMaster;
import alluxio.master.journal.JournalReader;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalLogWriter;
import alluxio.master.journal.ufs.UfsJournalReader;
import alluxio.master.metastore.MetastoreType;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.proto.journal.Journal;
import alluxio.testutils.AlluxioOperationThread;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.util.CommonUtils;
import alluxio.util.URIUtils;
import alluxio.util.WaitForOptions;
import alluxio.wire.BackupStatus;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Integration test for backing up and restoring alluxio master.
 */
public final class JournalBackupIntegrationTest extends BaseIntegrationTest {
  public MultiProcessCluster mCluster;
  private static final int GET_PRIMARY_INDEX_TIMEOUT_MS = 30000;
  private static final int PRIMARY_KILL_TIMEOUT_MS = 30000;
  private static final int WAIT_NODES_REGISTERED_MS = 30000;

  @Rule
  public ConfigurationRule mConf = new ConfigurationRule(new HashMap<PropertyKey, Object>() {
    {
      put(PropertyKey.USER_METRICS_COLLECTION_ENABLED, false);
    }
  }, Configuration.modifiableGlobal());

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
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS)
        // Masters become primary faster
        .addProperty(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT, "3sec")
        // Disable backup delegation
        .addProperty(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, false)
        .build();
    backupRestoreTest(true);
  }

  @Test
  public void backupRestoreMetastore_Heap() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.BACKUP_RESTORE_METASSTORE_HEAP)
        .setClusterName("backupRestoreMetastore_Heap")
        .setNumMasters(1)
        .setNumWorkers(1)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS)
        // Masters become primary faster
        .addProperty(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT, "1sec")
        .addProperty(PropertyKey.MASTER_METASTORE, MetastoreType.HEAP)
        // Disable backup delegation
        .addProperty(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, false)
        .build();
    backupRestoreMetaStoreTest();
  }

  @Test
  public void backupRestoreMetastore_Rocks() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.BACKUP_RESTORE_METASSTORE_ROCKS)
        .setClusterName("backupRestoreMetastore_Rocks")
        .setNumMasters(1)
        .setNumWorkers(1)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS)
        // Masters become primary faster
        .addProperty(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT, "1sec")
        .addProperty(PropertyKey.MASTER_METASTORE, MetastoreType.ROCKS)
        // Disable backup delegation
        .addProperty(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, false)
        .build();
    backupRestoreMetaStoreTest();
  }

  @Test
  public void emergencyBackup() throws Exception {
    emergencyBackupCore(1);
  }

  @Test
  public void emergencyBackupHA() throws Exception {
    emergencyBackupCore(3);
  }

  private void emergencyBackupCore(int numMasters) throws Exception {
    TemporaryFolder backupFolder = new TemporaryFolder();
    backupFolder.create();
    List<PortCoordination.ReservedPort> ports1 = numMasters > 1
        ? PortCoordination.BACKUP_EMERGENCY_HA_1 : PortCoordination.BACKUP_EMERGENCY_1;
    String clusterName1 = numMasters > 1 ? "emergencyBackup_HA_1" : "emergencyBackup_1";
    mCluster = MultiProcessCluster.newBuilder(ports1)
        .setClusterName(clusterName1)
        .setNumMasters(numMasters)
        .setNumWorkers(0)
        // Masters become primary faster, will be ignored in non HA case
        .addProperty(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT, "1sec")
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS)
        .addProperty(PropertyKey.MASTER_METASTORE, MetastoreType.ROCKS)
        .addProperty(PropertyKey.MASTER_BACKUP_DIRECTORY, backupFolder.getRoot())
        .addProperty(PropertyKey.MASTER_JOURNAL_BACKUP_WHEN_CORRUPTED, true)
        // Disable backup delegation
        .addProperty(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, false)
        .build();
    mCluster.start();
    final int numFiles = 10;
    // create normal uncorrupted journal
    for (int i = 0; i < numFiles; i++) {
      mCluster.getFileSystemClient().createFile(new AlluxioURI("/normal-file-" + i));
    }
    mCluster.stopMasters();
    // corrupt journal
    URI journalLocation = new URI(mCluster.getJournalDir());
    UfsJournal fsMaster =
        new UfsJournal(URIUtils.appendPathOrDie(journalLocation, "FileSystemMaster"),
            new NoopMaster(), 0, Collections::emptySet);
    fsMaster.start();
    fsMaster.gainPrimacy();
    long nextSN = 0;
    try (UfsJournalReader reader = new UfsJournalReader(fsMaster, true)) {
      while (reader.advance() != JournalReader.State.DONE) {
        nextSN++;
      }
    }
    try (UfsJournalLogWriter writer = new UfsJournalLogWriter(fsMaster, nextSN)) {
      Journal.JournalEntry entry = Journal.JournalEntry.newBuilder()
          .setSequenceNumber(nextSN)
          .setDeleteFile(alluxio.proto.journal.File.DeleteFileEntry.newBuilder()
              .setId(4563728) // random non-zero ID number (zero would delete the root)
              .setPath("/nonexistant")
              .build())
          .build();
      writer.write(entry);
      writer.flush();
    }
    // this should fail and create backup(s)
    mCluster.startMasters();
    // wait for backup file(s) to be created
    // successful backups leave behind one .gz file and one .gz.complete file
    CommonUtils.waitFor("backup file(s) to be created automatically",
        () -> 2 * numMasters == Objects.requireNonNull(backupFolder.getRoot().list()).length,
        WaitForOptions.defaults().setInterval(500).setTimeoutMs(30_000));
    List<String> backupFiles = Arrays.stream(Objects.requireNonNull(backupFolder.getRoot().list()))
            .filter(s -> s.endsWith(".gz")).collect(Collectors.toList());
    assertEquals(numMasters, backupFiles.size());
    // create new cluster
    List<PortCoordination.ReservedPort> ports2 = numMasters > 1
        ? PortCoordination.BACKUP_EMERGENCY_HA_2 : PortCoordination.BACKUP_EMERGENCY_2;
    String clusterName2 = numMasters > 1 ? "emergencyBackup_HA_2" : "emergencyBackup_2";
    // verify that every backup contains all the entries
    for (String backupFile : backupFiles) {
      mCluster = MultiProcessCluster.newBuilder(ports2)
          .setClusterName(String.format("%s_%s", clusterName2, backupFile))
          .setNumMasters(numMasters)
          .setNumWorkers(0)
          .addProperty(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT, "1sec")
          .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS)
          // change metastore type to ensure backup is independent of metastore type
          .addProperty(PropertyKey.MASTER_METASTORE, MetastoreType.HEAP)
          .addProperty(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP,
              Paths.get(backupFolder.getRoot().toString(), backupFile))
          .build();
      mCluster.start();
      // test that the files were restored from the backup properly
      for (int i = 0; i < numFiles; i++) {
        boolean exists = mCluster.getFileSystemClient().exists(new AlluxioURI("/normal-file-" + i));
        assertTrue(exists);
      }
      mCluster.stopMasters();
      mCluster.notifySuccess();
    }
    backupFolder.delete();
  }

  @Test
  public void syncRootOnBackupRestore() throws Exception {
    TemporaryFolder temporaryFolder = new TemporaryFolder();
    temporaryFolder.create();
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.BACKUP_SYNC_ON_RESTORE)
        .setClusterName("syncRootOnBackupRestore")
        .setNumMasters(1)
        .setNumWorkers(1)
        .addProperty(PropertyKey.MASTER_BACKUP_DIRECTORY, temporaryFolder.getRoot())
        .addProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH)
        // this test uses NEVER as the metadata load type to ensure that the UFS sync is
        // performed due to the invalidation associated with restoring a backup, as opposed to
        // performed automatically under some other metadata load types
        .addProperty(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataPType.NEVER)
        // Disable backup delegation
        .addProperty(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, false)
        .build();
    mCluster.start();

    mCluster.getFileSystemClient().createDirectory(new AlluxioURI("/in_backup"));
    BackupStatus backup =
        mCluster.getMetaMasterClient().backup(BackupPRequest.getDefaultInstance());
    UUID id = backup.getBackupId();
    while (backup.getState() != BackupState.Completed) {
      backup = mCluster.getMetaMasterClient().getBackupStatus(id);
    }
    mCluster.getFileSystemClient().createDirectory(new AlluxioURI("/NOT_in_backup"));
    mCluster.stopMasters();
    // give it an empty journal and start from backup
    mCluster.updateMasterConf(PropertyKey.MASTER_JOURNAL_FOLDER,
        temporaryFolder.newFolder().getAbsolutePath());
    mCluster.updateMasterConf(PropertyKey.MASTER_METASTORE_DIR,
        temporaryFolder.newFolder().getAbsolutePath());
    mCluster.updateMasterConf(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP,
        backup.getBackupUri().getPath());
    mCluster.startMasters();
    List<URIStatus> statuses = mCluster.getFileSystemClient().listStatus(new AlluxioURI("/"));
    assertEquals(2, statuses.size());
    mCluster.notifySuccess();
    temporaryFolder.delete();
  }

  @Test
  public void syncContentsOnBackupRestore() throws Exception {
    TemporaryFolder temporaryFolder = new TemporaryFolder();
    temporaryFolder.create();
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.BACKUP_CONTENT_ON_RESTORE)
        .setClusterName("syncContentOnBackupRestore")
        .setNumMasters(1)
        .setNumWorkers(1)
        .addProperty(PropertyKey.MASTER_BACKUP_DIRECTORY, temporaryFolder.getRoot())
        .addProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH)
        // Disable backup delegation
        .addProperty(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, false)
        .build();
    mCluster.start();

    AlluxioURI f = new AlluxioURI("/in_backup");
    try (FileOutStream inBackup = mCluster.getFileSystemClient().createFile(f)) {
      inBackup.write("data".getBytes());
    }

    BackupStatus backup =
        mCluster.getMetaMasterClient().backup(BackupPRequest.getDefaultInstance());
    UUID id = backup.getBackupId();
    while (backup.getState() != BackupState.Completed) {
      backup = mCluster.getMetaMasterClient().getBackupStatus(id);
    }

    mCluster.getFileSystemClient().delete(f);
    String modifiedData = "modified data";
    try (FileOutStream overwriteInBackup = mCluster.getFileSystemClient().createFile(f)) {
      overwriteInBackup.write(modifiedData.getBytes());
    }
    mCluster.stopMasters();
    // give it an empty journal and start from backup
    mCluster.updateMasterConf(PropertyKey.MASTER_JOURNAL_FOLDER,
        temporaryFolder.newFolder().getAbsolutePath());
    mCluster.updateMasterConf(PropertyKey.MASTER_METASTORE_DIR,
        temporaryFolder.newFolder().getAbsolutePath());
    mCluster.updateMasterConf(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP,
        backup.getBackupUri().getPath());
    mCluster.startMasters();

    try (FileInStream inStream = mCluster.getFileSystemClient().openFile(f)) {
      byte[] bytes = new byte[modifiedData.length()];
      int read = inStream.read(bytes);
      // if the invalidation is not set during the backup restore only the length of the old
      // contents ("data") will be read (instead of reading the new contents "modified data")
      assertEquals(modifiedData.length(), read);
      assertEquals(modifiedData, new String(bytes));
    }
    mCluster.notifySuccess();
    temporaryFolder.delete();
  }

  // This test needs to stop and start master many times, so it can take up to a minute to complete.
  @Test
  public void backupRestoreEmbedded() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.BACKUP_RESTORE_EMBEDDED)
        .setClusterName("backupRestoreEmbedded")
        .setNumMasters(3)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED)
        // Disable backup delegation
        .addProperty(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, false)
        .build();
    backupRestoreTest(true);
  }

  @Test
  public void backupRestoreSingleMaster() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.BACKUP_RESTORE_SINGLE)
        .setClusterName("backupRestoreSingle")
        .setNumMasters(1)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS)
        // Disable backup delegation
        .addProperty(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, false)
        .build();
    backupRestoreTest(false);
  }

  // Tests various protocols and configurations for backup delegation.
  @Test
  public void backupDelegationProtocol() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.BACKUP_DELEGATION_PROTOCOL)
        .setClusterName("backupDelegationProtocol")
        .setNumMasters(3)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS)
        // Masters become primary faster
        .addProperty(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT, "1sec")
        // For faster backup role handshake.
        .addProperty(PropertyKey.MASTER_BACKUP_CONNECT_INTERVAL_MIN, "100ms")
        .addProperty(PropertyKey.MASTER_BACKUP_CONNECT_INTERVAL_MAX, "100ms")
        // Enable backup delegation
        .addProperty(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, true)
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
            String.format("Unexpected error while getting backup status: %s", e));
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
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS)
        // Masters become primary faster
        .addProperty(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT, "1sec")
        // For faster backup role handshake.
        .addProperty(PropertyKey.MASTER_BACKUP_CONNECT_INTERVAL_MIN, "100ms")
        .addProperty(PropertyKey.MASTER_BACKUP_CONNECT_INTERVAL_MAX, "100ms")
        // Enable backup delegation
        .addProperty(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, true)
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
            String.format("Unexpected error while getting backup status: %s", e));
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
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS)
        // Masters become primary faster
        .addProperty(PropertyKey.ZOOKEEPER_SESSION_TIMEOUT, "1sec")
        // Enable backup delegation
        .addProperty(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, true)
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
            .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED)
            // Enable backup delegation
            .addProperty(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, true)
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
            String.format("Backup failed with unexpected error: %s", e2));
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
        MasterClientContext.newBuilder(ClientContext.create(Configuration.global()))
            .setMasterInquireClient(cluster.getMasterInquireClient())
            .build());
  }

  private BlockMasterClient getBlockClient(MultiProcessCluster cluster) {
    return new RetryHandlingBlockMasterClient(
        MasterClientContext.newBuilder(ClientContext.create(Configuration.global()))
            .setMasterInquireClient(cluster.getMasterInquireClient()).build());
  }
}
