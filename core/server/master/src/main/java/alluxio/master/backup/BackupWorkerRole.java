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
import alluxio.ClientContext;
import alluxio.ProcessUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.BackupException;
import alluxio.grpc.BackupPRequest;
import alluxio.grpc.BackupState;
import alluxio.grpc.CopycatMessageServerGrpc;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterClientContext;
import alluxio.master.MasterInquireClient;
import alluxio.master.journal.CatchupFuture;
import alluxio.master.journal.raft.transport.CopycatGrpcClientConnection;
import alluxio.master.journal.raft.transport.CopycatGrpcConnection;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BackupStatus;

import com.google.common.base.Preconditions;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Implementation of {@link BackupRole} for secondary mode.
 */
public class BackupWorkerRole extends AbstractBackupRole {
  private static final Logger LOG = LoggerFactory.getLogger(BackupWorkerRole.class);

  // Constant timeout for when suspend request is not followed by a backup request.
  private static final long BACKUP_ABORT_AFTER_SUSPEND_TIMEOUT_MS = 15000;
  // Constant timeout for journal transition before backup.
  private static final long BACKUP_ABORT_AFTER_TRANSITION_TIMEOUT_MS = 30000;
  // Minimum retry wait time between each connection attempt to leader.
  private final long mLeaderConnectionIntervalMin;
  // Maximum retry wait time between each connection attempt to leader.
  private final long mLeaderConnectionIntervalMax;
  // Interval at which backup progress will be sent to the leader.
  private final long mBackupHeartbeatIntervalMs;

  /** Connection with the leader. */
  private Connection mLeaderConnection;
  /** Close listener for leader connection. */
  private Listener<Connection> mLeaderConnectionCloseListener;

  /** Future to control ongoing backup. */
  private Future<?> mBackupFuture;

  /** Scheduled future for sending backup progress. */
  private Future<?> mBackupProgressFuture;
  /** Scheduled future for timing out various backup stages. */
  private ScheduledFuture<?> mBackupTimeoutTask;

  /**
   * Creates a new backup worker.
   *
   * @param masterContext the master context
   */
  public BackupWorkerRole(CoreMasterContext masterContext) {
    super(masterContext);
    // Read properties.
    mBackupHeartbeatIntervalMs =
        ServerConfiguration.getMs(PropertyKey.MASTER_BACKUP_HEARTBEAT_INTERVAL);
    mLeaderConnectionIntervalMin =
        ServerConfiguration.getMs(PropertyKey.MASTER_BACKUP_CONNECT_INTERVAL_MIN);
    mLeaderConnectionIntervalMax =
        ServerConfiguration.getMs(PropertyKey.MASTER_BACKUP_CONNECT_INTERVAL_MAX);
  }

  @Override
  public void start() {
    LOG.info("Starting backup-worker role.");
    // Submit a task to establish and maintain connection with the leader.
    mExecutorService.submit(() -> {
      establishConnectionToLeader();
    });
  }

  @Override
  public void stop() throws IOException {
    LOG.info("Stopping backup-worker role.");
    // Cancel suspend timeout.
    if (mBackupTimeoutTask != null && !mBackupTimeoutTask.isDone()) {
      mBackupTimeoutTask.cancel(true);
    }
    // Cancel ongoing backup task.
    if (mBackupFuture != null) {
      mBackupFuture.cancel(true);
    }
    // Cancel heartbeat task.
    if (mBackupProgressFuture != null) {
      mBackupProgressFuture.cancel(true);
    }
    // Close leader close listener.
    // This will ensure, connection won't be re-established when closed during stop.
    if (mLeaderConnectionCloseListener != null) {
      mLeaderConnectionCloseListener.close();
    }
    // Close the connection with the leader.
    if (mLeaderConnection != null) {
      mLeaderConnection.close();
      mLeaderConnection = null;
    }
    // Stopping the base after because closing connection uses the base executor.
    super.stop();
  }

  @Override
  public Map<ServiceType, GrpcService> getRoleServices() {
    return Collections.emptyMap();
  }

  @Override
  public BackupStatus backup(BackupPRequest request) throws AlluxioException {
    throw new IllegalStateException("Backup-worker role can't serve RPCs");
  }

  @Override
  public BackupStatus getBackupStatus() throws AlluxioException {
    throw new IllegalStateException("Backup-worker role can't serve RPCs");
  }

  /**
   * Handler for suspend message. It's used in secondary master.
   */
  private void handleSuspendJournalsMessage(BackupSuspendMessage suspendMsg) {
    LOG.info("Received suspend message: {}", suspendMsg.toString());
    Preconditions.checkState(!mBackupTracker.inProgress(), "Backup in progress");

    try {
      mJournalSystem.suspend();
      LOG.info("Suspended journals for backup.");
    } catch (IOException e) {
      String failMessage = "Failed to suspended journals for backup.";
      LOG.error(failMessage, e);
      throw new RuntimeException(failMessage, e);
    }
    // Schedule a timeout task to resume journals if protocol is not followed by the leader.
    mBackupTimeoutTask = mTaskScheduler.schedule(() -> {
      LOG.info("Resuming journals as backup request hasn't been received.");
      enforceResumeJournals();
    }, BACKUP_ABORT_AFTER_SUSPEND_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  /**
   * Handler for backup request message. It's used in secondary master.
   */
  private void handleRequestMessage(BackupRequestMessage requestMsg) {
    LOG.info("Received backup message: {}", requestMsg.toString());
    Preconditions.checkState(!mBackupTracker.inProgress(), "Backup in progress");

    // Reset backup tracker.
    mBackupTracker.reset();
    mBackupTracker.updateState(BackupState.Initiating);
    mBackupTracker.updateHostname(NetworkAddressUtils.getLocalHostName(
        (int) ServerConfiguration.global().getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)));

    // Start sending backup progress to leader.
    startHeartbeatThread();

    // Cancel timeout task created by suspend message handler.
    if (!mBackupTimeoutTask.cancel(true)) {
      mBackupTracker.updateError(new BackupException("Journal has ben resumed due to time-out."));
      return;
    }

    // Spawn a task for advancing journals to target sequences, then taking the backup.
    mBackupFuture = mExecutorService.submit(() -> {
      // Mark state as transitioning.
      mBackupTracker.updateState(BackupState.Transitioning);
      try {
        LOG.info(
            "Initiating catching up of journals to consistent sequences before starting backup. {}",
            requestMsg.getJournalSequences());
        CatchupFuture catchupFuture = mJournalSystem.catchup(requestMsg.getJournalSequences());
        CompletableFuture.runAsync(() -> catchupFuture.waitTermination())
            .get(BACKUP_ABORT_AFTER_TRANSITION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        mBackupTracker.updateError(new BackupException("Failed to catch-up journals.", e));
        // Don't resume journals if interrupted.
        // Catch-up future can be interrupted only when master becomes primary during backup.
        if (!(e instanceof InterruptedException)) {
          enforceResumeJournals();
        }
        return;
      }
      LOG.info("Journal transition completed. Taking a backup.");
      try {
        mBackupTracker.updateState(BackupState.Running);
        AlluxioURI backupUri =
            takeBackup(requestMsg.getBackupRequest(), mBackupTracker.getEntryCounter());
        mBackupTracker.updateBackupUri(backupUri);
        mBackupTracker.updateState(BackupState.Completed);
        // Wait until backup heartbeats are completed.
        try {
          mBackupProgressFuture.get();
        } catch (Exception e) {
          LOG.warn("Failed to wait for backup heartbeat completion. ", e);
        }
      } catch (IOException e) {
        mBackupTracker.updateError(new BackupException("Backup failed at worker.", e));
      } finally {
        enforceResumeJournals();
      }
    });
  }

  /**
   * Resumes the journals. Crashes the process if resume fails.
   */
  private void enforceResumeJournals() {
    try {
      mJournalSystem.resume();
    } catch (Throwable e) {
      ProcessUtils.fatalError(LOG, e, "Failed to resume journals.");
    }
  }

  /**
   * Creates a task for sending periodic heartbeats to leader backup master.
   * Heartbeats are stopped once the backup is finished.
   */
  private void startHeartbeatThread() {
    // Cancel if existing heartbeat task is active.
    if (mBackupProgressFuture != null && !mBackupProgressFuture.isDone()) {
      mBackupProgressFuture.cancel(true);
    }
    // Submit new heartbeat task.
    mBackupProgressFuture = mExecutorService.submit(() -> {
      while (true) {
        // No need to check result because heartbeat will be sent regardless.
        mBackupTracker.waitUntilFinished(mBackupHeartbeatIntervalMs, TimeUnit.MILLISECONDS);
        try {
          sendMessageBlocking(mLeaderConnection,
              new BackupHeartbeatMessage(mBackupTracker.getCurrentStatus()));
        } catch (Exception e) {
          LOG.warn("Failed to send heartbeat to backup-leader: {}. Error: {}", mLeaderConnection,
              e);
        }
        // Stop heartbeats if backup finished.
        if (mBackupTracker.isFinished()) {
          break;
        }
      }
    });
  }

  /**
   * Prepares new leader connection.
   */
  private void activateLeaderConnection(Connection leaderConnection) throws IOException {
    // Register connection error listener.
    leaderConnection.onException((error) -> {
      LOG.warn("Backup-leader connection failed.", error);
    });
    // Register connection close listener.
    mLeaderConnectionCloseListener = leaderConnection.onClose((connection) -> {
      LOG.info("Backup-leader connection closed. {}", connection);
      // Cancel ongoing backup if leader is lost.
      if (mBackupFuture != null && !mBackupFuture.isDone()) {
        LOG.warn("Cancelling ongoing backup as backup-leader is lost.");
        mBackupFuture.cancel(true);
        mBackupTracker.reset();
      }
      // Re-establish leader connection to a potentially new leader.
      mExecutorService.submit(() -> {
        establishConnectionToLeader();
      });
    });
    // Register message handlers under catalyst context.
    try {
      mCatalystContext.execute(() -> {
        // Register suspend message handler.
        leaderConnection.handler(BackupSuspendMessage.class,
            (Consumer<BackupSuspendMessage>) request -> handleSuspendJournalsMessage(request));
        // Register backup message handler.
        leaderConnection.handler(BackupRequestMessage.class,
            (Consumer<BackupRequestMessage>) request -> handleRequestMessage(request));
        // Send handshake message to introduce connection to leader.
        leaderConnection.sendAndReceive(new BackupHandshakeMessage(
            NetworkAddressUtils.getLocalHostName((int) ServerConfiguration.global()
                .getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS))));
      }).get();
    } catch (InterruptedException ie) {
      throw new RuntimeException("Interrupted while activating backup-leader connection.");
    } catch (ExecutionException ee) {
      leaderConnection.close();
      throw new IOException("Failed to activate backup-leader connection.", ee.getCause());
    }
  }

  /**
   * Establishes a connection with the leader backup master.
   */
  private void establishConnectionToLeader() {
    // Create unending retry policy for establishing connection with the leader backup master.
    RetryPolicy infiniteRetryPolicy = new ExponentialBackoffRetry(
        (int) mLeaderConnectionIntervalMin, (int) mLeaderConnectionIntervalMax, Integer.MAX_VALUE);

    while (infiniteRetryPolicy.attempt()) {
      try {
        // Create inquire client to determine leader address.
        MasterInquireClient inquireClient =
            MasterClientContext.newBuilder(ClientContext.create(ServerConfiguration.global()))
                .build().getMasterInquireClient();
        // Get leader address.
        Address leaderAddress = new Address(inquireClient.getPrimaryRpcAddress());

        // Create a new gRPC channel for connection with leader.
        GrpcChannel channel = GrpcChannelBuilder
            .newBuilder(
                GrpcServerAddress.create(leaderAddress.host(), leaderAddress.socketAddress()),
                ServerConfiguration.global())
            .setClientType("BackupWorker").setSubject(mServerUserState.getSubject())
            .build();

        // Create stub for receiving stream from leader.
        CopycatMessageServerGrpc.CopycatMessageServerStub messageClientStub =
            CopycatMessageServerGrpc.newStub(channel);

        // Create a client connection to leader.
        CopycatGrpcConnection leaderConnection = new CopycatGrpcClientConnection(mCatalystContext,
            mExecutorService, channel, mCatalystRequestTimeout);
        leaderConnection.setTargetObserver(messageClientStub.connect(leaderConnection));

        // Activate the connection.
        activateLeaderConnection(leaderConnection);
        mLeaderConnection = leaderConnection;
        LOG.info("Established connection to backup-leader: {}", leaderAddress);
        break;
      } catch (Exception e) {
        LOG.warn("Failed to establish connection to backup-leader. Error:{}. Attempt:{}",
            e.toString(), infiniteRetryPolicy.getAttemptCount());
      }
    }
  }
}
