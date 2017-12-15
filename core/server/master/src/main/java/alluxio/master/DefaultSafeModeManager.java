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

import com.google.common.base.Preconditions;
import org.apache.curator.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Manages safe mode state for Alluxio master.
 */
public class DefaultSafeModeManager implements SafeModeManager {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultSafeModeManager.class);

  /** The executor used for running maintenance threads for the master. */
  private ScheduledExecutorService mScheduledExecutorService;

  /**
   * Safe mode state and task.
   */
  private boolean mIsInSafeMode;
  private LeaveSafeModeTask mLeaveSafeModeTask;
  private ScheduledFuture<?> mScheduledTask;

  /**
   * @param scheduledExecutorService an executor service for schedule maintenance tasks
   */
  public DefaultSafeModeManager(ScheduledExecutorService scheduledExecutorService) {
    mScheduledExecutorService =
        Preconditions.checkNotNull(scheduledExecutorService, "scheduledExecutorService");
  }

  /**
   * Creates {@link DefaultSafeModeManager} with default ScheduledExecutorService.
   */
  public DefaultSafeModeManager() {
    this(ThreadUtils.newSingleThreadScheduledExecutor(DefaultSafeModeManager.class.getSimpleName()));
  }

  @Override
  public synchronized void enterSafeMode() {
    LOG.info("Entering safe mode.");
    if (mLeaveSafeModeTask == null) {
      mLeaveSafeModeTask = new LeaveSafeModeTask();
    }

    if (mScheduledTask != null && !mScheduledTask.isDone()) {
      mScheduledTask.cancel(true);
    }

    mIsInSafeMode = true;

    mScheduledTask = mScheduledExecutorService.schedule(mLeaveSafeModeTask,
        Configuration.getMs(PropertyKey.MASTER_SAFEMODE_WAIT), TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean isInSafeMode() {
    return mIsInSafeMode;
  }

  private void leaveSafeMode() {
    LOG.info("Leaving safe mode.");
    mIsInSafeMode = false;
  }

  private class LeaveSafeModeTask implements Runnable {
    @Override
    public void run() {
      leaveSafeMode();
    }
  }
}
