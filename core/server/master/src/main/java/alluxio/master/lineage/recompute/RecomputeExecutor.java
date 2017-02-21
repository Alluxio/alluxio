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

package alluxio.master.lineage.recompute;

import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.UnexpectedAlluxioException;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.lineage.meta.Lineage;
import alluxio.master.lineage.meta.LineageStateUtils;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A periodical executor that detects lost files and launches recompute jobs.
 */
@ThreadSafe
public final class RecomputeExecutor implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(RecomputeExecutor.class);

  private static final int DEFAULT_RECOMPUTE_LAUNCHER_POOL_SIZE = 10;
  private final RecomputePlanner mPlanner;
  private final FileSystemMaster mFileSystemMaster;
  /** The thread pool to launch recompute jobs. */
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
  @ThreadSafe
  final class RecomputeLauncher implements Runnable {
    private final RecomputePlan mPlan;

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
            } catch (UnexpectedAlluxioException e) {
              LOG.error("the lost file {} can not be freed", fileId, e);
            } catch (FileDoesNotExistException e) {
              LOG.error("the lost file {} does not exist", fileId, e);
            } catch (InvalidPathException e) {
              LOG.error("the lost file {} is invalid", fileId, e);
            } catch (AccessControlException e) {
              LOG.error("the lost file {} cannot be accessed", fileId, e);
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
