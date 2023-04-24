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

import alluxio.grpc.JobMasterMetaCommand;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.wire.Address;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * If a job master is detected as a standby job master. It will set up its JobMasterSync and
 * use its {@link RetryHandlingMetaMasterMasterClient} to register to the primary job master,
 * then maintain a heartbeat with the primary.
 */
@NotThreadSafe
public final class JobMasterSync implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(JobMasterSync.class);
  private static final long UNINITIALIZED_MASTER_ID = -1L;

  /** The address of this standby job master. */
  private final Address mMasterAddress;

  /** Client for communication with the primary master. */
  private final RetryHandlingJobMasterMasterClient mMasterClient;

  /** The ID of this standby master. */
  private final AtomicReference<Long> mMasterId = new AtomicReference<>(UNINITIALIZED_MASTER_ID);

  /**
   * Creates a new instance of {@link MetaMasterSync}.
   *
   * @param masterAddress the master address
   * @param masterClient the meta master client
   */
  public JobMasterSync(Address masterAddress, RetryHandlingJobMasterMasterClient masterClient) {
    mMasterAddress = masterAddress;
    mMasterClient = masterClient;
  }

  /**
   * Heartbeats to the leader master node.
   */
  @Override
  public void heartbeat(long timeout) {
    JobMasterMetaCommand command = null;
    try {
      if (mMasterId.get() == UNINITIALIZED_MASTER_ID) {
        setIdAndRegister();
      }
      command = mMasterClient.heartbeat(mMasterId.get());
      handleCommand(command);
    } catch (IOException e) {
      // An error occurred, log and ignore it or error if heartbeat timeout is reached
      if (command == null) {
        LOG.error("Failed to receive primary master heartbeat command.", e);
      } else {
        LOG.error("Failed to execute primary master heartbeat command: {}", command, e);
      }
      mMasterClient.disconnect();
    }
  }

  /**
   * Handles a leader master command.
   *
   * @param cmd the command to execute
   */
  private void handleCommand(JobMasterMetaCommand cmd) throws IOException {
    if (cmd == null) {
      return;
    }
    switch (cmd) {
      case MetaCommand_Nothing:
        break;
      // Primary master requests re-registration
      case MetaCommand_Register:
        setIdAndRegister();
        break;
      // Unknown request
      case MetaCommand_Unknown:
        LOG.error("Master heartbeat sends unknown command {}", cmd);
        break;
      default:
        throw new RuntimeException("Un-recognized command from primary master " + cmd);
    }
  }

  /**
   * Sets the master id and registers with the Alluxio leader master.
   */
  private void setIdAndRegister() throws IOException {
    LOG.info("Prepare to register to primary job master");
    mMasterId.set(mMasterClient.getId(mMasterAddress));
    LOG.info("Received job master ID {}", mMasterId.get());
    mMasterClient.register(mMasterId.get());
    LOG.info("Registered with primary job master");
  }

  @Override
  public void close() {}
}
