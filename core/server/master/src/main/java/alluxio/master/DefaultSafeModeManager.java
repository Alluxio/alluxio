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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.clock.Clock;
import alluxio.clock.ElapsedTimeClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages safe mode state for Alluxio master.
 */
public class DefaultSafeModeManager implements SafeModeManager {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultSafeModeManager.class);

  private final Clock mClock;

  /**
   * Safe mode state. The value will be null if master is not in safe mode, or a nanosecond time
   * point indicating when master will stop waiting for workers and leave safe mode.
   */
  private AtomicReference<Long> mWorkerConnectWaitEndTime = new AtomicReference<>();

  /**
   * Creates {@link DefaultSafeModeManager} with default clock.
   */
  public DefaultSafeModeManager() {
    this(new ElapsedTimeClock());
  }

  /**
   * Creates {@link DefaultSafeModeManager} with given clock.
   * @param clock a {@link Clock} for calculating elapsed time
   */
  public DefaultSafeModeManager(Clock clock) {
    mClock = clock;
  }

  @Override
  public void notifyRpcServerStarted() {
    // updates end time after which master will leave safe mode
    long waitTime = Configuration.getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME);
    LOG.info(String.format("Entering safe mode. Expect leaving safe mode after %dms", waitTime));
    mWorkerConnectWaitEndTime.set(mClock.millis() + waitTime);
  }

  @Override
  public boolean isInSafeMode() {
    // lazily updates safe mode state upon inquiry
    Long endTime = mWorkerConnectWaitEndTime.get();

    // bails out early before expensive clock checks
    if (endTime == null) {
      return false;
    }
    if (mClock.millis() < endTime) {
      return true;
    }
    return !mWorkerConnectWaitEndTime.compareAndSet(endTime, null);
  }
}
