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

package alluxio.worker.block;

import alluxio.ProcessUtils;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.FailedToAcquireRegisterLeaseException;
import alluxio.grpc.Command;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The block master sync thread when workers are registered to all masters.
 * With respect to behaviors, this implementation differs from {@link BlockMasterSync} in:
 * 1. The registration takes place asynchronously and the caller can poll the registration state.
 *  We need to make the process async because when standby master read is enabled, workers have to
 *  register to all masters and these registrations can happen concurrently to speed up the process.
 * 2. A registration fail doesn't throw a fatal exception. Instead, it retries endlessly.
 *  This is because a standby master registration failure
 *  should not a soft failure and can be retried later.
 */
@NotThreadSafe
public class SpecificMasterBlockSync implements HeartbeatExecutor, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(SpecificMasterBlockSync.class);
  private static final long ACQUIRE_LEASE_WAIT_MAX_DURATION =
      Configuration.getMs(PropertyKey.WORKER_REGISTER_LEASE_RETRY_MAX_DURATION);

  private final SocketAddress mMasterAddress;

  /**
   * The worker registration state.
   * If the state is NOT_REGISTERED, heartbeat will trigger a registration.
   * During the registration process, the state will be set to REGISTERING.
   * When the registration is done, the state will be set to REGISTERED.
   * When the sync receives a registration command from the master during the heartbeat,
   * the state will be reset to NOT_REGISTERED and the sync will attempt to register it again
   * in the next heartbeat.
   */
  private volatile WorkerMasterState mWorkerState = WorkerMasterState.NOT_REGISTERED;

  /**
   * An async service to remove block.
   */
  private final AsyncBlockRemover mAsyncBlockRemover;

  /**
   * The worker ID for the worker. This may change if the master asks the worker to re-register.
   */
  private final AtomicReference<Long> mWorkerId;

  /**
   * Client for all master communication.
   */
  private final BlockMasterClient mMasterClient;

  /**
   * The net address of the worker.
   */
  private final WorkerNetAddress mWorkerAddress;

  /**
   * The helper instance for sync related methods.
   */
  private final BlockMasterSyncHelper mBlockMasterSyncHelper;

  /**
   * The block worker responsible for interacting with Alluxio and UFS storage.
   */
  private final BlockWorker mBlockWorker;
  /**
   * Last System.currentTimeMillis() timestamp when a heartbeat successfully completed.
   */
  private long mLastSuccessfulHeartbeatMs = 0;

  private final BlockHeartbeatReporter mBlockHeartbeatReporter;

  /**
   * Creates a new instance of {@link SpecificMasterBlockSync}.
   *
   * @param blockWorker the {@link BlockWorker} this syncer is updating to
   * @param masterClient the block master client
   * @param heartbeatReporter the heartbeat reporter
   */
  public SpecificMasterBlockSync(
      BlockWorker blockWorker,
      BlockMasterClient masterClient, BlockHeartbeatReporter heartbeatReporter)
      throws IOException {
    mBlockWorker = blockWorker;
    mWorkerId = blockWorker.getWorkerId();
    mWorkerAddress = blockWorker.getWorkerAddress();
    mMasterClient = masterClient;
    mAsyncBlockRemover = new AsyncBlockRemover(mBlockWorker);
    mBlockMasterSyncHelper = new BlockMasterSyncHelper(mMasterClient);
    mMasterAddress = masterClient.getRemoteSockAddress();
    mBlockHeartbeatReporter = heartbeatReporter;
  }

  private void registerWithMaster() {
    RetryPolicy retry = createEndlessRetry();
    while (retry.attempt()) {
      try {
        LOG.info("Registering with master {}", mMasterAddress);
        registerWithMasterInternal();
        LOG.info("Finished registration with {}", mMasterAddress);
        return;
      } catch (Exception e) {
        LOG.error("Failed to register with master {}, error {}, retry count {} Will retry...",
            mMasterAddress, e, retry.getAttemptCount());
        mWorkerState = WorkerMasterState.NOT_REGISTERED;
      }
    }
    // Should not reach here because the retry is indefinite
    ProcessUtils.fatalError(LOG, new RuntimeException(),
        "Failed to register with master %s", mMasterAddress);
  }

  protected void registerWithMasterInternal()
      throws IOException, FailedToAcquireRegisterLeaseException {
    // The target master is not necessarily the one that allocated the workerID
    LOG.info("Notify the master {} about the workerID {}", mMasterAddress, mWorkerId);
    mMasterClient.addWorkerId(mWorkerId.get(), mWorkerAddress);

    BlockStoreMeta storeMeta = mBlockWorker.getStoreMetaFull();

    try {
      mBlockMasterSyncHelper.tryAcquireLease(mWorkerId.get(), storeMeta);
    } catch (FailedToAcquireRegisterLeaseException e) {
      if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
        throw new RuntimeException(String.format("Master register lease timeout exceeded: %dms",
            ACQUIRE_LEASE_WAIT_MAX_DURATION));
      }
      throw e;
    }
    mWorkerState = WorkerMasterState.REGISTERING;
    mBlockMasterSyncHelper.registerToMaster(mWorkerId.get(), storeMeta);

    mWorkerState = WorkerMasterState.REGISTERED;
    Metrics.WORKER_MASTER_REGISTRATION_SUCCESS_COUNT.inc();
    mLastSuccessfulHeartbeatMs = CommonUtils.getCurrentMs();
  }

  private RetryPolicy createEndlessRetry() {
    return new ExponentialBackoffRetry(
        1000, 60 * 1000, Integer.MAX_VALUE);
  }

  @Override
  public synchronized void heartbeat() throws InterruptedException {
    if (mWorkerState == WorkerMasterState.NOT_REGISTERED) {
      // Not registered because:
      // 1. The worker just started, we kick off the 1st registration here.
      // 2. Master sends a registration command during
      // the heartbeat and resets the registration state.
      LOG.info("The worker needs to register with master {}", mMasterAddress);
      // This will retry indefinitely and essentially block here if the master is not ready
      registerWithMaster();
      LOG.info("BlockMasterSync to master {} has started", mMasterAddress);
    }
    if (mWorkerState == WorkerMasterState.REGISTERING) {
      return;
    }

    RetryPolicy endlessRetry = createEndlessRetry();
    while (endlessRetry.attempt()) {
      BlockHeartbeatReport report = mBlockHeartbeatReporter.generateReportAndClear();
      boolean success = false;
      try {
        beforeHeartbeat();
        success = mBlockMasterSyncHelper.heartbeat(
            mWorkerId.get(), report,
            mBlockWorker.getStoreMeta(), this::handleMasterCommand);
      } catch (Exception e) {
        LOG.error("Failed to receive master heartbeat command. worker id {}", mWorkerId, e);
      }
      if (success) {
        mLastSuccessfulHeartbeatMs = CommonUtils.getCurrentMs();
        break;
      } else {
        mBlockHeartbeatReporter.revert(report);
        LOG.warn(
            "Heartbeat failed, worker id {}, worker host {} # of attempts {}, last success ts {}",
            mWorkerId.get(), mWorkerAddress.getHost(), endlessRetry.getAttemptCount(),
            mLastSuccessfulHeartbeatMs);
      }
    }
  }

  protected void beforeHeartbeat() {
  }

  @Override
  public void close() {
    mAsyncBlockRemover.shutDown();
    mMasterClient.close();
  }

  /**
   * @return if the worker has registered with the master successfully
   */
  public boolean isRegistered() {
    return mWorkerState == WorkerMasterState.REGISTERED;
  }

  /**
   * Handles a master command. The command is one of Unknown, Nothing, Register, Free, or Delete.
   * This call will block until the command is complete.
   *
   * @param cmd the command to execute
   * @throws IOException               if I/O errors occur
   * @throws ConnectionFailedException if connection fails
   */
  private void handleMasterCommand(Command cmd) throws IOException, ConnectionFailedException {
    if (cmd == null) {
      return;
    }
    switch (cmd.getCommandType()) {
      // Currently unused
      case Delete:
        break;
      // Master requests blocks to be removed from Alluxio managed space.
      case Free:
        mAsyncBlockRemover.addBlocksToDelete(cmd.getDataList());
        break;
      // No action required
      case Nothing:
        break;
      // Master requests re-registration
      case Register:
        mWorkerState = WorkerMasterState.NOT_REGISTERED;
        break;
      // Unknown request
      case Unknown:
        LOG.error("Master heartbeat sends unknown command {}", cmd);
        break;
      default:
        throw new RuntimeException("Un-recognized command from master " + cmd);
    }
  }

  enum WorkerMasterState {
    REGISTERED,
    NOT_REGISTERED,
    REGISTERING
  }

  /**
   * Metrics.
   */
  public static final class Metrics {
    private static final Counter WORKER_MASTER_REGISTRATION_SUCCESS_COUNT
        = MetricsSystem.counter(MetricKey.WORKER_MASTER_REGISTRATION_SUCCESS_COUNT.getName());
  }
}
