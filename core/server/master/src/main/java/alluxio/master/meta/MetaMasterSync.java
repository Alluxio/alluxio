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

package alluxio.master.meta;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.retry.ExponentialTimeBoundedRetry;
import alluxio.retry.RetryUtils;
import alluxio.thrift.MetaCommand;
import alluxio.wire.Address;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * If a master is detected as a standby master. It will set up its MetaMasterSync and manage
 * its own {@link MetaMasterMasterClient} which helps communicate with the leader master.
 *
 * When running, the standby master will send its heartbeat to the leader master.
 * The leader master may respond to the heartbeat with a command which will be executed.
 * After which, the task will wait for the elapsed time since its last heartbeat
 * has reached the heartbeat interval. Then the cycle will continue.
 */
@NotThreadSafe
public final class MetaMasterSync implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(MetaMasterSync.class);
  /** We set a large retry day which means retry forever. */
  private static final long RETRY_DAY = 100000;
  private static final long UNINITIALIZED_MASTER_ID = -1L;

  /** Milliseconds between heartbeats before a timeout. */
  private final int mHeartbeatTimeoutMs;

  /** Last System.currentTimeMillis() timestamp when a heartbeat successfully completed. */
  private long mLastSuccessfulHeartbeatMs;

  /** The address of this standby master. */
  private final Address mMasterAddress;

  /** Client for communication with the leader master. */
  private final MetaMasterMasterClient mMasterClient;

  /** The ID of this standby master. */
  private final AtomicReference<Long> mMasterId = new AtomicReference<>(UNINITIALIZED_MASTER_ID);

  /**
   * Creates a new instance of {@link MetaMasterSync}.
   *
   * @param masterAddress the master address
   * @param masterClient the meta master client
   */
  public MetaMasterSync(Address masterAddress, MetaMasterMasterClient masterClient) {
    mMasterAddress = masterAddress;
    mMasterClient = masterClient;
    mHeartbeatTimeoutMs = (int) Configuration.getMs(PropertyKey.MASTER_HEARTBEAT_TIMEOUT_MS);
    mLastSuccessfulHeartbeatMs = System.currentTimeMillis() - mHeartbeatTimeoutMs;
  }

  /**
   * Heartbeats to the leader master node.
   */
  @Override
  public void heartbeat() {
    MetaCommand command = null;
    try {
      if (mMasterId.get() == UNINITIALIZED_MASTER_ID) {
        setIdAndRegister();
      }
      command = mMasterClient.heartbeat(mMasterId.get());
      handleCommand(command);
      mLastSuccessfulHeartbeatMs = System.currentTimeMillis();
    } catch (IOException e) {
      // An error occurred, log and ignore it or error if heartbeat timeout is reached
      if (command == null) {
        LOG.error("Failed to receive leader master heartbeat command.", e);
      } else {
        LOG.error("Failed to execute leader master heartbeat command: {}", command, e);
      }
      mMasterClient.disconnect();
      if (mHeartbeatTimeoutMs > 0) {
        if (System.currentTimeMillis() - mLastSuccessfulHeartbeatMs >= mHeartbeatTimeoutMs) {
          if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
            throw new RuntimeException("Leader Master heartbeat timeout exceeded: "
                + mHeartbeatTimeoutMs);
          }
          LOG.error("Leader Master heartbeat timeout exceeded: " + mHeartbeatTimeoutMs);
        }
      }
    }
  }

  /**
   * Handles a leader master command.
   *
   * @param cmd the command to execute
   * @throws IOException if I/O errors occur
   */
  private void handleCommand(MetaCommand cmd) throws IOException {
    if (cmd == null) {
      return;
    }
    switch (cmd) {
      case Nothing:
        break;
      // Leader master requests re-registration
      case Register:
        setIdAndRegister();
        break;
      // Unknown request
      case Unknown:
        LOG.error("Master heartbeat sends unknown command {}", cmd);
        break;
      default:
        throw new RuntimeException("Un-recognized command from leader master " + cmd);
    }
  }

  /**
   * Sets the master id and registers with the Alluxio leader master.
   */
  private void setIdAndRegister() throws IOException {
    try {
      RetryUtils.retry("get master id",
          () -> mMasterId.set(mMasterClient.getId(mMasterAddress)),
          ExponentialTimeBoundedRetry.builder()
              .withMaxDuration(Duration.ofDays(RETRY_DAY))
              .withInitialSleep(Duration.ofMillis(100))
              .withMaxSleep(Duration.ofSeconds(5))
              .build());
    } catch (Exception e) {
      throw new RuntimeException("Failed to get a master id from leader master: " + e.getMessage());
    }

    mMasterClient.register(mMasterId.get(),
        Configuration.getConfiguration(PropertyKey.Scope.MASTER));
  }

  @Override
  public void close() {}
}
