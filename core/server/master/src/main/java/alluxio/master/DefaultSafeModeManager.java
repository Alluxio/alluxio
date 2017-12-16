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
import alluxio.Constants;
import alluxio.PropertyKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages safe mode state for Alluxio master.
 */
public class DefaultSafeModeManager implements SafeModeManager {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultSafeModeManager.class);

  /**
   * Safe mode state. The value will be null if master is not in safe mode, or a nanosecond time
   * point indicating when master will stop waiting for workers and leave safe mode.
   */
  private volatile Long mWorkerConnectWaitEndTime;

  @Override
  public void enterSafeMode() {
    long waitTime = Configuration.getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME);
    LOG.info(String.format("Entering safe mode. Expect leaving safe mode after %dms", waitTime));
    mWorkerConnectWaitEndTime = System.nanoTime() / Constants.MS_NANO + waitTime;
  }

  @Override
  public boolean isInSafeMode() {
    // lazily updates safe mode state upon inquiry
    Long endTime = mWorkerConnectWaitEndTime;
    if (endTime != null && System.nanoTime() / Constants.MS_NANO > endTime) {
      mWorkerConnectWaitEndTime = null;
    }

    return mWorkerConnectWaitEndTime != null;
  }
}
