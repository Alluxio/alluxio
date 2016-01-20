/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.lineage.recompute;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;

import tachyon.Constants;
import tachyon.exception.FileDoesNotExistException;
import tachyon.heartbeat.HeartbeatExecutor;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.lineage.meta.Lineage;
import tachyon.master.lineage.meta.LineageStateUtils;
import tachyon.util.ThreadFactoryUtils;

/**
 * A periodical executor that detects lost files and launches recompute jobs.
 */
public final class RecomputeExecutor implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final int DEFAULT_RECOMPUTE_LAUNCHER_POOL_SIZE = 10;
  private final RecomputePlanner mPlanner;
  private final FileSystemMaster mFileSystemMaster;
  /** The thread pool to launch recompute jobs */
  private final ExecutorService mRecomputeLauncherService =
      Executors.newFixedThreadPool(DEFAULT_RECOMPUTE_LAUNCHER_POOL_SIZE,
          ThreadFactoryUtils.build("recompute-launcher-%d", true));

  /**
   * Creates a new instance of {@link RecomputeExecutor}.
   *
   * @param planner recompute planner
   * @param fileSystemMaster the file system master
   */
  public RecomputeExecutor(RecomputePlanner planner, FileSystemMaster fileSystemMaster) {
    mPlanner = Preconditions.checkNotNull(planner);
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
  }

  @Override
  public void heartbeat() {
    heartbeatWithFuture();
  }

  @Override
  public void close() {
    mRecomputeLauncherService.shutdown();
  }

  /**
   * A version of {@code heartbeat} which returns a {@link Future} representing completion of the
   * recompute plan. This is especially useful for tests.
   *
   * @return the {@code Future} representing completion of the recompute plan
   */
  Future<?> heartbeatWithFuture() {
    RecomputePlan plan = mPlanner.plan();
    if (plan != null && !plan.isEmpty()) {
      return mRecomputeLauncherService.submit(new RecomputeLauncher(plan));
    }
    return Futures.<Void>immediateFuture(null);
  }

  /**
   * Thread to launch the recompute jobs in a given plan.
   */
  final class RecomputeLauncher implements Runnable {
    private RecomputePlan mPlan;

    /**
     * Creates a new instance of {@link RecomputeLauncher}.
     *
     * @param plan the recompute plan
     */
    RecomputeLauncher(RecomputePlan plan) {
      mPlan = Preconditions.checkNotNull(plan);
    }

    @Override
    public void run() {
      for (Lineage lineage : mPlan.getLineageToRecompute()) {
        // empty all the lost files
        try {
          for (Long fileId : LineageStateUtils.getLostFiles(lineage,
              mFileSystemMaster.getFileSystemMasterView())) {
            try {
              mFileSystemMaster.resetFile(fileId);
            } catch (FileDoesNotExistException e) {
              LOG.error("the lost file {} is invalid", fileId, e);
            }
          }
        } catch (FileDoesNotExistException e) {
          LOG.error("an output file of lineage {} does not exist", lineage.getId(), e);
        }

        boolean success = lineage.getJob().run();
        if (!success) {
          LOG.error("Failed to recompute job {}", lineage.getJob());
        }
      }
    }
  }
}
