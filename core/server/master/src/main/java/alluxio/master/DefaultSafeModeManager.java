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

package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.clock.ElapsedTimeClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicMarkableReference;

/**
 * Manages safe mode state for Alluxio master.
 */
public class DefaultSafeModeManager implements SafeModeManager {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultSafeModeManager.class);

  private final Clock mClock;

  /**
   * Safe mode state. The mark indicates whether master is in safe mode, the reference stores
   * time point in millisecond indicating when master started waiting for workers.
   */
  private final AtomicMarkableReference<Long> mWorkerConnectWaitStartTimeMs =
      new AtomicMarkableReference<>(null, true);

  /**
   * Creates {@link DefaultSafeModeManager} with default clock.
   */
  public DefaultSafeModeManager() {
    this(new ElapsedTimeClock());
  }

  /**
   * Creates {@link DefaultSafeModeManager} with given clock.
   *
   * @param clock a {@link Clock} for calculating elapsed time
   */
  public DefaultSafeModeManager(Clock clock) {
    mClock = clock;
  }

  @Override
  public void notifyPrimaryMasterStarted() {
    mWorkerConnectWaitStartTimeMs.set(null, true);
  }

  @Override
  public void notifyRpcServerStarted() {
    // updates start time when Alluxio master waits for workers to register
    long waitTime = ServerConfiguration.getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME);
    LOG.info(String.format("Rpc server started, waiting %dms for workers to register", waitTime));
    mWorkerConnectWaitStartTimeMs.set(mClock.millis(), true);
  }

  @Override
  public boolean isInSafeMode() {
    // bails out early before expensive clock checks
    if (!mWorkerConnectWaitStartTimeMs.isMarked()) {
      return false;
    }

    Long startTime = mWorkerConnectWaitStartTimeMs.getReference();
    if (startTime == null) {
      // master has not started waiting for workers yet
      return true;
    }

    // lazily updates safe mode state upon inquiry
    long waitTime = ServerConfiguration.getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME);
    if (mClock.millis() - startTime < waitTime) {
      return true;
    }

    if (mWorkerConnectWaitStartTimeMs.compareAndSet(startTime, null, true, false)) {
      LOG.debug("Exiting safe mode.");
    }

    return mWorkerConnectWaitStartTimeMs.isMarked();
  }
}
