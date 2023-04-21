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
 * 2. A registration failure doesn't throw a fatal exception. Instead, it retries endlessly.
 *  This is because a standby master registration failure
 *  should be a soft failure and can be retried later.
 */
@NotThreadSafe
public class SpecificMasterBlockSync implements HeartbeatExecutor, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(SpecificMasterBlockSync.class);
  private static final long ACQUIRE_LEASE_WAIT_MAX_DURATION =
      Configuration.getMs(PropertyKey.WORKER_REGISTER_LEASE_RETRY_MAX_DURATION);

  private final long mWorkerBlockHeartbeatReportSizeThreshold =
      Configuration.getInt(PropertyKey.WORKER_BLOCK_HEARTBEAT_REPORT_SIZE_THRESHOLD);

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
  private volatile WorkerMasterRegistrationState mWorkerState =
      WorkerMasterRegistrationState.NOT_REGISTERED;

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
        // The content in the report can be cleared because registration will
        // report these block information anyways.
        mBlockHeartbeatReporter.clear();
        registerWithMasterInternal();
        LOG.info("Finished registration with {}", mMasterAddress);
        return;
      } catch (Exception e) {
        LOG.error("Failed to register with master {}, error {}, retry count {} Will retry...",
            mMasterAddress, e, retry.getAttemptCount());
        mWorkerState = WorkerMasterRegistrationState.NOT_REGISTERED;
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
    mMasterClient.notifyWorkerId(mWorkerId.get(), mWorkerAddress);
    // TODO(elega) If worker registration to all masters happens at the same time,
    // this might cause worker OOM issues because each block sync thread will hold a BlockStoreMeta
    // instance during the registration.
    // If this happens, consider limiting the worker registration concurrency,
    // e.g. register the worker to masters one by one.
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
    mWorkerState = WorkerMasterRegistrationState.REGISTERING;
    mBlockMasterSyncHelper.registerToMaster(mWorkerId.get(), storeMeta);

    mWorkerState = WorkerMasterRegistrationState.REGISTERED;
    Metrics.WORKER_MASTER_REGISTRATION_SUCCESS_COUNT.inc();
    mLastSuccessfulHeartbeatMs = CommonUtils.getCurrentMs();
  }

  private RetryPolicy createEndlessRetry() {
    return new ExponentialBackoffRetry(
        1000, 60 * 1000, Integer.MAX_VALUE);
  }

  @Override
  public synchronized void heartbeat(long runLimit) throws InterruptedException {
    if (mWorkerState == WorkerMasterRegistrationState.NOT_REGISTERED) {
      // Not registered because:
      // 1. The worker just started, we kick off the 1st registration here.
      // 2. Master sends a registration command during
      // the heartbeat and resets the registration state. (e.g. master restarted)
      // 3. The heartbeat message becomes too big that we decide to fall back to a full re-register
      LOG.info("The worker needs to register with master {}", mMasterAddress);
      // This will retry indefinitely and essentially block here if the master is not ready
      registerWithMaster();
      LOG.info("BlockMasterSync to master {} has started", mMasterAddress);
    }
    if (mWorkerState == WorkerMasterRegistrationState.REGISTERING) {
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
        LOG.warn(
            "Heartbeat failed, worker id {}, worker host {} # of attempts {}, last success ts {}",
            mWorkerId.get(), mWorkerAddress.getHost(), endlessRetry.getAttemptCount(),
            mLastSuccessfulHeartbeatMs);
        if (report.getBlockChangeCount() >= mWorkerBlockHeartbeatReportSizeThreshold) {
          // If the report becomes too big, merging it back to the reporter might cause OOM issue.
          // We throw away the result and let the worker re-register with the master.
          mWorkerState = WorkerMasterRegistrationState.NOT_REGISTERED;
          return;
        } else {
          mBlockHeartbeatReporter.mergeBack(report);
        }
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
    return mWorkerState == WorkerMasterRegistrationState.REGISTERED;
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
        mWorkerState = WorkerMasterRegistrationState.NOT_REGISTERED;
        break;
      // Unknown request
      case Unknown:
        LOG.error("Master heartbeat sends unknown command {}", cmd);
        break;
      default:
        throw new RuntimeException("Un-recognized command from master " + cmd);
    }
  }

  /**
   * Metrics.
   */
  public static final class Metrics {
    private static final Counter WORKER_MASTER_REGISTRATION_SUCCESS_COUNT
        = MetricsSystem.counter(MetricKey.WORKER_MASTER_REGISTRATION_SUCCESS_COUNT.getName());
  }
}
