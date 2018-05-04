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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Task that carries out the necessary standby master to leader master communications, including
 * register and heartbeat. This class manages its own {@link MetaMasterMasterClient}.
 *
 * When running, this task first requests master configuration from the standby master,
 * then sends it to the leader master. After which, the task will wait for the elapsed time
 * since its last heartbeat has reached the heartbeat interval. Then the cycle will continue.
 *
 * If the task fails to heartbeat to the leader master, it will destroy its old master client
 * and recreate it before retrying.
 */
@NotThreadSafe
public final class MetaMasterSync implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(MetaMasterSync.class);

  /**
   * The master ID for the master.
   * This may change if the leader master asks the standby master to re-register.
   */
  private final AtomicReference<Long> mMasterId;

  /** The hostname of the master. */
  private final String mMasterHostname;

  /** Milliseconds between heartbeats before a timeout. */
  private final int mHeartbeatTimeoutMs;

  /** Client for all master communication. */
  private final MetaMasterMasterClient mMasterClient;

  /** Last System.currentTimeMillis() timestamp when a heartbeat successfully completed. */
  private long mLastSuccessfulHeartbeatMs;

  /**
   * Creates a new instance of {@link MetaMasterSync}.
   *
   * @param masterId the master id of the master, assigned by the meta master
   * @param masterHostname the hostname of the master
   * @param masterClient the Alluxio master client
   */
  public MetaMasterSync(AtomicReference<Long>  masterId,
      String masterHostname, MetaMasterMasterClient masterClient) throws IOException {
    mMasterId = masterId;
    mMasterHostname = masterHostname;
    mMasterClient = masterClient;
    mHeartbeatTimeoutMs = (int) Configuration.getMs(PropertyKey.MASTER_HEARTBEAT_TIMEOUT_MS);

    registerWithMaster();
    mLastSuccessfulHeartbeatMs = System.currentTimeMillis();
  }

  /**
   * Registers with the Alluxio master. This should be called
   * before the continuous heartbeat thread begins.
   */
  private void registerWithMaster() throws IOException {
    mMasterClient.register(mMasterId.get(),
        Configuration.getConfiguration(PropertyKey.Scope.MASTER));
  }

  /**
   * Heartbeats to the leader master node.
   */
  @Override
  public void heartbeat() {
    try {
      boolean shouldReRegister = mMasterClient.heartbeat(mMasterId.get());
      if (shouldReRegister) {
        mMasterId.set(mMasterClient.getId(mMasterHostname));
        registerWithMaster();
      }
      mLastSuccessfulHeartbeatMs = System.currentTimeMillis();
    } catch (IOException e) {
      mMasterClient.disconnect();
      if (mHeartbeatTimeoutMs > 0) {
        if (System.currentTimeMillis() - mLastSuccessfulHeartbeatMs >= mHeartbeatTimeoutMs) {
          if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
            throw new RuntimeException("Master heartbeat timeout exceeded: " + mHeartbeatTimeoutMs);
          }
          LOG.error("Master heartbeat timeout exceeded: " + mHeartbeatTimeoutMs);
          System.exit(-1);
        }
      }
    }
  }

  @Override
  public void close() {}
}
