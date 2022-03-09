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

package alluxio.master.job.common;

import alluxio.exception.ExceptionMessage;
import alluxio.exception.JobDoesNotExistException;
import alluxio.job.wire.Status;
import alluxio.master.job.plan.PlanCoordinator;
import alluxio.master.job.plan.PlanTracker;

import com.beust.jcommander.internal.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Progress information for a distributed command.
 */
public class CmdProgress {
  private static final Logger LOG = LoggerFactory.getLogger(CmdProgress.class);
  private long mJobControlId;
  private Map<Long, JobProgress> mCmdProgressMap;

  /**
   * Constructor for CmdProgress.
   * @param jobControlId
   */
  public CmdProgress(long jobControlId) {
    mJobControlId = jobControlId;
    mCmdProgressMap = Maps.newHashMap();
  }

  /**
   * Get job control id.
   * @return id
   */
  public long getControlId() {
    return mJobControlId;
  }

  /**
   * Check the job progress.
   * @param jobId
   * @return job progress info
   */
  public JobProgress checkJobProgress(long jobId) throws JobDoesNotExistException {
    if (mCmdProgressMap.containsKey(jobId)) {
      return mCmdProgressMap.get(jobId);
    } else {
      throw new JobDoesNotExistException(
              ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobId));
    }
  }

  /**
   * Create or update progress for a given job ID.
   * @param planTracker
   * @param jobId
   * @param fileCount
   * @param fileSize
   * @param verbose
   */
  public void createOrUpdateChildProgress(PlanTracker planTracker, long jobId,
                                     long fileCount, long fileSize, boolean verbose)
          throws JobDoesNotExistException {
    PlanCoordinator planCoordinator = planTracker.getCoordinator(jobId);
    if (planCoordinator != null) {
      JobProgress progress = mCmdProgressMap.getOrDefault(jobId,
         new JobProgress(jobId));
      progress.update(planCoordinator, verbose, fileCount, fileSize);
      mCmdProgressMap.put(jobId, progress);
      System.out.println("createOrUpdateChildProgress: " + progress.toString());
    } else {
      throw new JobDoesNotExistException(
              ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobId));
    }
  }

  /**
   * Consolidate status for the CMD job.
   * @return status for a cmd job
   */
  public Status consolidateStatus() {
    int completed = 0;
    for (Map.Entry<Long, JobProgress> entry : mCmdProgressMap.entrySet()) {
      Long id = entry.getKey();
      System.out.println("consolidating status now, id " + id);

      JobProgress progress = entry.getValue();
      Status s = progress.getStatus();
      if (s == Status.CANCELED) {
        return Status.CANCELED;
      } else if (s == Status.FAILED) {
        return Status.FAILED;
      } else if (s == Status.COMPLETED) {
        completed++;
      }
    }

    if (completed == mCmdProgressMap.size()) {
      return Status.COMPLETED;
    }

    return Status.RUNNING;
  }

  /**
   * list all progress.
   */
  public void listAllProgress() {
    LOG.info("jobControlId = " + mJobControlId);
    mCmdProgressMap.forEach((id, progress) -> {
      LOG.info(String.format("Child job id is %d, progress is %s", id, progress.toString()));
    });
  }
}
