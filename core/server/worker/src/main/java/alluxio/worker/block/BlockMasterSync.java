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
import alluxio.exception.FailedToAcquireRegisterLeaseException;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The block master sync thread when standby master read feature is disabled.
 * This implementation will register with master synchronously and throw a fatal error
 * if the registration fails.
 */
@NotThreadSafe
public final class BlockMasterSync extends AbstractBlockMasterSync {
  private static final Logger LOG = LoggerFactory.getLogger(BlockMasterSync.class);

  /**
   * Creates a new instance of {@link BlockMasterSync}.
   *
   * @param blockWorker the {@link BlockWorker} this syncer is updating to
   * @param workerId  worker id of the worker, assigned by the block master
   * @param workerAddress the net address of the worker
   * @param masterClientPool the Alluxio master client pool
   * @param heartbeatReporter the block heartbeat reporter
   */
  public BlockMasterSync(
      BlockWorker blockWorker, AtomicReference<Long> workerId,
      WorkerNetAddress workerAddress, BlockMasterClientPool masterClientPool,
      BlockHeartbeatReporter heartbeatReporter)
      throws IOException {
    super(blockWorker, workerId, workerAddress, masterClientPool, heartbeatReporter);

    registerWithMaster();
    mLastSuccessfulHeartbeatMs = CommonUtils.getCurrentMs();
  }

  /**
   * Registers with the Alluxio master. This should be called before the
   * continuous heartbeat thread begins.
   */
  protected void registerWithMaster() throws IOException {
    try {
      tryAcquireLease();
    } catch (FailedToAcquireRegisterLeaseException e) {
      mMasterClient.disconnect();
      if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
        throw new RuntimeException(String.format("Master register lease timeout exceeded: %dms",
            ACQUIRE_LEASE_WAIT_MAX_DURATION));
      }
      ProcessUtils.fatalError(LOG, "Master register lease timeout exceeded: %dms",
          ACQUIRE_LEASE_WAIT_MAX_DURATION);
    }
    registerToMaster();
  }

  /**
   * Heartbeats to the master node about the change in the worker's managed space.
   */
  @Override
  public void heartbeat() {
    try {
      doHeartbeat();
    } catch (TimeoutException e) {
      if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
        throw new RuntimeException(e.getMessage());
      }
      ProcessUtils.fatalError(LOG, e.getMessage());
    }
  }

  @Override
  protected void handleMasterRegisterCommand() throws IOException {
    mWorkerId.set(mMasterClient.getId(mWorkerAddress));
    registerWithMaster();
  }
}
