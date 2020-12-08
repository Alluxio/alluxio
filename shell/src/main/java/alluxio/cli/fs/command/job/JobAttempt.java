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

package alluxio.cli.fs.command.job;

import alluxio.client.job.JobMasterClient;
import alluxio.job.JobConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.retry.RetryPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Abstract class for handling submission for a job.
 */
public abstract class JobAttempt {
  private static final Logger LOG = LoggerFactory.getLogger(JobAttempt.class);

  protected final JobMasterClient mClient;
  protected final RetryPolicy mRetryPolicy;

  private Long mJobId;

  protected JobAttempt(JobMasterClient client, RetryPolicy retryPolicy) {
    mClient = client;
    mRetryPolicy = retryPolicy;
  }

  /**
   * Runs the job.
   * @return true if an attempt was made, false if attempts ran out
   */
  public boolean run() {
    if (mRetryPolicy.attempt()) {
      mJobId = null;
      try {
        mJobId = mClient.run(getJobConfig());
      } catch (IOException e) {
        LOG.warn("Failed to start job", e);
        System.out.println(String.format("Failed to start job with error: %s", e.getMessage()));
        // Do nothing. This will be counted as a failed attempt
      }
      return true;
    }
    logFailed();
    return false;
  }

  /**
   * Returns the status of the job attempt.
   * @return True if finished successfully or cancelled, False if FAILED and should be retried,
   *              null if the status should be checked again later
   */
  public Status check() {
    if (mJobId == null) {
      return Status.FAILED;
    }

    JobInfo jobInfo;
    try {
      jobInfo = mClient.getJobStatus(mJobId);
    } catch (IOException e) {
      LOG.warn("Failed to get status for job (jobId={})", mJobId, e);
      return Status.FAILED;
    }

    // This make an assumption that this job tree only goes 1 level deep
    boolean finished = true;
    for (JobInfo child : jobInfo.getChildren()) {
      if (!child.getStatus().isFinished()) {
        finished = false;
        break;
      }
    }

    if (finished) {
      if (jobInfo.getStatus().equals(Status.FAILED)) {
        logFailedAttempt(jobInfo);
      } else if (jobInfo.getStatus().equals(Status.COMPLETED)) {
        logCompleted();
      }
      return jobInfo.getStatus();
    }
    return Status.RUNNING;
  }

  protected abstract JobConfig getJobConfig();

  protected abstract void logFailedAttempt(JobInfo jobInfo);

  protected abstract void logFailed();

  protected abstract void logCompleted();
}
