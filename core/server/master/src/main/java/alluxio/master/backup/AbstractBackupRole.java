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

package alluxio.master.backup;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.BackupPRequest;
import alluxio.master.BackupManager;
import alluxio.master.CoreMasterContext;
import alluxio.master.journal.JournalSystem;
import alluxio.master.transport.GrpcMessagingConnection;
import alluxio.master.transport.GrpcMessagingContext;
import alluxio.resource.CloseableResource;
import alluxio.security.user.UserState;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.PathUtils;

import com.google.common.io.Closer;
import io.atomix.catalyst.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract backup role implementation. It provides common infrastructure for leader/worker backup
 * roles.
 */
public abstract class AbstractBackupRole implements BackupRole {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractBackupRole.class);

  // Request timeout for backup messages.
  protected final long mCatalystRequestTimeout;

  /** The manager of all ufs. */
  protected final UfsManager mUfsManager;

  /** Task scheduler. */
  protected ScheduledExecutorService mTaskScheduler;

  /** The executor service. */
  protected ExecutorService mExecutorService;

  /** Catalyst context used for sending/receiving messages. */
  protected GrpcMessagingContext mGrpcMessagingContext;

  /** Journal system. */
  protected JournalSystem mJournalSystem;

  /** Backup Manager. */
  protected BackupManager mBackupManager;

  /** User information for server. */
  protected UserState mServerUserState;

  /** Used to track backup status. */
  protected BackupTracker mBackupTracker;

  /** Whether the role is active. */
  protected boolean mRoleClosed = false;

  /**
   * Creates a BackupMaster.
   *
   * @param masterContext master context
   */
  public AbstractBackupRole(CoreMasterContext masterContext) {
    // Executor service will be used for driving the backup and connections.
    mExecutorService =
        Executors.newCachedThreadPool(ThreadFactoryUtils.build("backup-executor-%d", true));
    // Task scheduler will be used for various backup related tasks.
    mTaskScheduler =
        Executors.newScheduledThreadPool(1, ThreadFactoryUtils.build("backup-task-%d", true));
    // Store master context objects.
    mUfsManager = masterContext.getUfsManager();
    mJournalSystem = masterContext.getJournalSystem();
    mBackupManager = masterContext.getBackupManager();
    mServerUserState = masterContext.getUserState();
    // Initialize messaging infra.
    initializeCatalystContext();
    // Read properties.
    mCatalystRequestTimeout =
        ServerConfiguration.getMs(PropertyKey.MASTER_BACKUP_TRANSPORT_TIMEOUT);
    // Initialize backup tracker.
    mBackupTracker = new BackupTracker();
  }

  /**
   * Initializes the catalyst context with known message types.
   */
  private void initializeCatalystContext() {
    Serializer messageSerializer = new Serializer().register(BackupRequestMessage.class)
        .register(BackupHeartbeatMessage.class).register(BackupSuspendMessage.class);
    mGrpcMessagingContext = new GrpcMessagingContext("backup-context-%d", messageSerializer);
  }

  /**
   * Used to send message via connection and wait until response is received. Response is discarded
   * since backup messages are one-way.
   */
  protected void sendMessageBlocking(GrpcMessagingConnection connection, Object message)
      throws IOException {
    try {
      mGrpcMessagingContext.execute(() -> {
        return connection.sendAndReceive(message);
      }).get().get(); // First get is for the task, second is for the messaging future.
    } catch (InterruptedException ie) {
      throw new RuntimeException("Interrupted while waiting for messaging to complete.");
    } catch (ExecutionException ee) {
      throw new IOException("Failed to send and receive message", ee.getCause());
    }
  }

  /**
   * Takes a backup on local master. Note: Master state must have been suspended or locked before
   * calling this.
   *
   * @param request the backup request
   * @param entryCounter counter to receive written entry count
   * @return URI of the backup
   */
  protected AlluxioURI takeBackup(BackupPRequest request, AtomicLong entryCounter)
      throws IOException {
    AlluxioURI backupUri;

    final Closer closer = Closer.create();
    // Acquire the UFS resource under which backup is being created.
    try (CloseableResource<UnderFileSystem> ufsResource =
        mUfsManager.getRoot().acquireUfsResource()) {
      // Get backup parent directory.
      String backupParentDir = request.hasTargetDirectory() ? request.getTargetDirectory()
          : ServerConfiguration.get(PropertyKey.MASTER_BACKUP_DIRECTORY);
      // Get ufs resource for backup.
      UnderFileSystem ufs = ufsResource.get();
      if (request.getOptions().getLocalFileSystem() && !ufs.getUnderFSType().equals("local")) {
        // TODO(lu) Support getting UFS based on type from UfsManager
        ufs = closer.register(UnderFileSystem.Factory.create("/",
            UnderFileSystemConfiguration.defaults(ServerConfiguration.global())));
      }
      // Ensure parent directory for backup.
      if (!ufs.isDirectory(backupParentDir)) {
        if (!ufs.mkdirs(backupParentDir,
            MkdirsOptions.defaults(ServerConfiguration.global()).setCreateParent(true))) {
          throw new IOException(String.format("Failed to create directory %s", backupParentDir));
        }
      }
      // Generate backup file path.
      Instant now = Instant.now();
      String backupFileName = String.format(BackupManager.BACKUP_FILE_FORMAT,
          DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneId.of("UTC")).format(now),
          now.toEpochMilli());
      String backupFilePath = PathUtils.concatPath(backupParentDir, backupFileName);
      // Calculate URI for the path.
      String rootUfs = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
      if (request.getOptions().getLocalFileSystem()) {
        rootUfs = "file:///";
      }
      backupUri = new AlluxioURI(new AlluxioURI(rootUfs), new AlluxioURI(backupFilePath));

      // Take the backup.
      try {
        try (OutputStream ufsStream = ufs.create(backupFilePath)) {
          // Create the backup from master state.
          mBackupManager.backup(ufsStream, entryCounter);
        }
        // Add a marker file indicating the file is completed.
        ufs.create(backupFilePath + ".complete").close();
      } catch (IOException e) {
        try {
          ufs.deleteExistingFile(backupFilePath);
        } catch (Exception e2) {
          LOG.error("Failed to clean up failed backup at {}", backupFilePath, e2);
          e.addSuppressed(e2);
        }
        throw new IOException(String.format("Backup failed. BackupUri: %s, LastEntryCount: %d",
            backupUri, entryCounter.get()), e);
      }
    } finally {
      closer.close();
    }
    return backupUri;
  }

  @Override
  public void close() throws IOException {
    // Stop task scheduler.
    if (mTaskScheduler != null) {
      mTaskScheduler.shutdownNow();
    }
    // Close messaging context.
    mGrpcMessagingContext.close();
    // Shutdown backup executor.
    mExecutorService.shutdownNow();
    // Reset backup tracker.
    mBackupTracker.reset();
    // Mark the role as closed.
    mRoleClosed = true;
  }
}
