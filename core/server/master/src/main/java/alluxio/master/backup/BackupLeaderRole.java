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
import alluxio.collections.ConcurrentHashSet;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.BackupAbortedException;
import alluxio.exception.BackupDelegationException;
import alluxio.exception.BackupException;
import alluxio.grpc.BackupPRequest;
import alluxio.grpc.BackupState;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.master.CoreMasterContext;
import alluxio.master.journal.raft.transport.CopycatMessageServiceClientHandler;
import alluxio.resource.LockResource;
import alluxio.security.authentication.ClientIpAddressInjector;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BackupStatus;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Connection;
import io.grpc.ServerInterceptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

/**
 * Implementation of {@link BackupRole} for primary mode.
 */
public class BackupLeaderRole extends AbstractBackupRole {
  private static final Logger LOG = LoggerFactory.getLogger(BackupLeaderRole.class);

  // Timeout after which backup will be abandoned if no heart-beat received.
  private final long mBackupAbandonTimeout;

  /** Metadata state pause lock. */
  private Lock mStatePauseLock;

  /** Scheduled future to time-put backups on leader. */
  private ScheduledFuture<?> mTimeoutBackupFuture;
  /** Time at which the last heart-beat was received by leader. */
  private Instant mLastHeartBeat;

  /** Future for local backup task. */
  private Future<?> mLocalBackupFuture;

  /** Used to mark remote connection through which delegated backup is being driven. */
  private Connection mRemoteBackupConnection;

  /** Backup-worker connections with the leader. */
  private Set<Connection> mBackupWorkerConnections = new ConcurrentHashSet<>();

  /** Used to store host names for backup-worker connections. */
  private Map<Connection, String> mBackupWorkerHostNames = new ConcurrentHashMap<>();

  /**
   * Creates a new backup leader.
   *
   * @param masterContext the master context
   */
  public BackupLeaderRole(CoreMasterContext masterContext) {
    super(masterContext);
    // Store state lock for pausing state change when necessary.
    mStatePauseLock = masterContext.pauseStateLock();
    // Read properties.
    mBackupAbandonTimeout =
        ServerConfiguration.getMs(PropertyKey.MASTER_BACKUP_ABANDON_TIMEOUT);
  }

  @Override
  public void start() throws IOException {
    LOG.info("Starting backup-leader role.");
  }

  @Override
  public void stop() throws IOException {
    LOG.info("Stopping backup-leader role.");
    // Reset existing stand-by connections.
    mBackupWorkerConnections.clear();
    mBackupWorkerHostNames.clear();
    // Cancel ongoing local backup task.
    if (mLocalBackupFuture != null && !mLocalBackupFuture.isDone()) {
      mLocalBackupFuture.cancel(true);
    }
    // Close each backup-worker connection.
    for (Connection conn : mBackupWorkerConnections) {
      conn.close();
    }
    // Stopping the base after because closing connection uses the base executor.
    super.stop();
  }

  @Override
  public Map<ServiceType, GrpcService> getRoleServices() {
    Map<ServiceType, GrpcService> services = new HashMap<>();
    services
        .put(ServiceType.META_MASTER_BACKUP_MESSAGING_SERVICE,
            new GrpcService(
                ServerInterceptors.intercept(
                    new CopycatMessageServiceClientHandler(
                        new Address(NetworkAddressUtils.getConnectAddress(
                            NetworkAddressUtils.ServiceType.MASTER_RPC,
                            ServerConfiguration.global())),
                        (conn) -> activateWorkerConnection(conn), mCatalystContext,
                        mExecutorService, mCatalystRequestTimeout),
                    new ClientIpAddressInjector())));
    return services;
  }

  @Override
  public BackupStatus backup(BackupPRequest request) throws AlluxioException {
    if (mBackupTracker.inProgress()) {
      throw new BackupException("Backup in progress");
    }

    // Whether to attempt to delegate backup to a backup worker.
    boolean delegateBackup =
        ServerConfiguration.getBoolean(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED)
            && ConfigurationUtils.isHaMode(ServerConfiguration.global());
    // Fail, if in HA mode and no masters available to delegate,
    // unless `AllowLeader` flag in the backup request is set.
    if (delegateBackup && mBackupWorkerHostNames.size() == 0) {
      if (request.getOptions().getAllowLeader()) {
        delegateBackup = false;
      } else {
        throw new BackupDelegationException("No master found to delegate backup.");
      }
    }
    // Initialize backup status.
    mBackupTracker.reset();
    mBackupTracker.updateState(BackupState.Initiating);
    mBackupTracker.updateHostname(NetworkAddressUtils.getLocalHostName(
        (int) ServerConfiguration.global().getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)));
    // Initiate the backup.
    if (delegateBackup) {
      // Fail the backup if delegation failed.
      if (!scheduleRemoteBackup(request)) {
        AlluxioException err = new BackupDelegationException("Failed to delegate backup.");
        mBackupTracker.updateError(err);
        // Throw here for failing the backup call.
        throw err;
      }
    } else {
      scheduleLocalBackup(request);
    }

    // Return current status if async.
    if (request.getOptions().getRunAsync()) {
      return mBackupTracker.getCurrentStatus();
    }

    // Wait until backup is completed.
    mBackupTracker.waitUntilFinished();
    return mBackupTracker.getCurrentStatus();
  }

  @Override
  public BackupStatus getBackupStatus() throws AlluxioException {
    return mBackupTracker.getCurrentStatus();
  }

  /**
   * Prepares new follower connection.
   */
  private void activateWorkerConnection(Connection workerConnection) {
    LOG.info("Backup-leader connected with backup-worker: {}", workerConnection);
    // Register handshake message handler.
    workerConnection.handler(BackupHandshakeMessage.class,
        (Consumer<BackupHandshakeMessage>) request -> {
          // Attach the connection before calling the handler.
          request.setConnection(workerConnection);
          handleHandshakeMessage(request);
        });
    // Register heartbeat message handler.
    workerConnection.handler(BackupHeartbeatMessage.class,
        (Consumer<BackupHeartbeatMessage>) request -> handleHeartbeatMessage(request));
    // Register connection error listener.
    workerConnection.onException((error) -> {
      LOG.warn(String.format("Backup-worker connection failed for %s.", workerConnection), error);
    });
    // Register connection close listener.
    workerConnection.onClose((conn) -> {
      LOG.info("Backup-worker connection closed for {}.", workerConnection);
      // Remove the connection when completed
      mBackupWorkerConnections.remove(conn);
      String backupWorkerHostname = mBackupWorkerHostNames.remove(conn);
      // Fail current backup if it was driven by the closed connection.
      if (mRemoteBackupConnection != null && mRemoteBackupConnection.equals(conn)) {
        LOG.warn("Abandoning current backup as backup-worker: {} is lost.", backupWorkerHostname);
        mBackupTracker.updateError(new BackupAbortedException("Backup-worker is lost."));
        mRemoteBackupConnection = null;
      }
    });
    // Store follower connection.
    // mBackupWorkerHostNames will be updated by handshake message.
    mBackupWorkerConnections.add(workerConnection);
  }

  /**
   * Schedule backup on this master.
   */
  private void scheduleLocalBackup(BackupPRequest request) {
    mLocalBackupFuture = mExecutorService.submit(() -> {
      try (LockResource lr = new LockResource(mStatePauseLock)) {
        try {
          mBackupTracker.updateState(BackupState.Running);
          AlluxioURI backupUri = takeBackup(request, mBackupTracker.getEntryCounter());
          mBackupTracker.updateBackupUri(backupUri);
          mBackupTracker.updateState(BackupState.Completed);
        } catch (IOException e) {
          mBackupTracker.updateError(new BackupException("Local backup failed.", e));
        }
      }
    });
  }

  /**
   * Delegates a backup to a worker.
   *
   * After successful delegation, follower will send regular heartbeats to leader for updating the
   * backup status.
   *
   * @return {@code true} if delegation successful
   */
  private boolean scheduleRemoteBackup(BackupPRequest request) {
    // Try to delegate backup to a follower.
    LOG.info("Scheduling backup at remote backup-worker.");
    for (Map.Entry<Connection, String> workerEntry : mBackupWorkerHostNames.entrySet()) {
      try {
        // Suspend journals on current follower.
        LOG.info("Suspending journals at backup-worker: {}", workerEntry.getValue());
        sendMessageBlocking(workerEntry.getKey(), new BackupSuspendMessage());
        // Get consistent journal sequences.
        Map<String, Long> journalSequences;
        try (LockResource stateLock = new LockResource(mStatePauseLock)) {
          journalSequences = mJournalSystem.getCurrentSequenceNumbers();
        }
        // Send backup request along with consistent journal sequences.
        LOG.info("Sending backup request to backup-worker: {}", workerEntry.getValue());
        sendMessageBlocking(workerEntry.getKey(),
            new BackupRequestMessage(request, journalSequences));
        // Delegation successful.
        mRemoteBackupConnection = workerEntry.getKey();
        // Start abandon timer.
        adjustAbandonTimeout(false);
        LOG.info("Delegated the backup to backup-worker: {}", workerEntry.getValue());
        return true;
      } catch (Exception e) {
        LOG.warn(String.format("Failed to delegate backup to a backup-worker: %s",
            workerEntry.getValue()), e);
      }
    }
    return false;
  }

  /**
   * Handles worker heart-beat message.
   */
  private synchronized void handleHandshakeMessage(BackupHandshakeMessage handshakeMsg) {
    LOG.info("Received handshake message:{}", handshakeMsg);
    mBackupWorkerHostNames.put(handshakeMsg.getConnection(),
        handshakeMsg.getBackupWorkerHostname());
  }

  /**
   * Handles worker heart-beat message.
   */
  private synchronized void handleHeartbeatMessage(BackupHeartbeatMessage heartbeatMsg) {
    LOG.info("Received heartbeat message:{}", heartbeatMsg);
    if (heartbeatMsg.getBackupStatus() != null) {
      // Process heart-beat.
      mBackupTracker.update(heartbeatMsg.getBackupStatus());
      // Adjust backup timeout.
      adjustAbandonTimeout(mBackupTracker.isFinished());
    }
  }

  /**
   * Used to set timer to abandon backup when not received a heart-beat for it.
   */
  private void adjustAbandonTimeout(boolean backupFinished) {
    // Mark last heart-beat time.
    mLastHeartBeat = Instant.now();
    // Cancel active timer.
    if (mTimeoutBackupFuture != null) {
      mTimeoutBackupFuture.cancel(true);
    }
    // Reschedule if backup is still in progress.
    if (!backupFinished) {
      mTimeoutBackupFuture = mTaskScheduler.schedule(() -> {
        Duration sinceLastHeartbeat = Duration.between(mLastHeartBeat, Instant.now());
        if (sinceLastHeartbeat.toMillis() >= mBackupAbandonTimeout) {
          // Abandon the backup.
          mBackupTracker.updateError(new BackupAbortedException("Backup timed out"));
        }
      }, mBackupAbandonTimeout, TimeUnit.MILLISECONDS);
    }
  }
}
