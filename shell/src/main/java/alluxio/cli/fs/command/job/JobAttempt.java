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
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract class for handling submission for a job.
 */
public abstract class JobAttempt {
  private static final Logger LOG = LoggerFactory.getLogger(JobAttempt.class);

  protected final JobMasterClient mClient;
  protected final RetryPolicy mRetryPolicy;

  private Long mJobId;
  protected Set<JobInfo> mFailedTasks;
  protected Set<String> mFailedFiles;

  protected JobAttempt(JobMasterClient client, RetryPolicy retryPolicy) {
    mClient = client;
    mRetryPolicy = retryPolicy;
    mFailedFiles = new HashSet<>();
  }

  /**
   * Runs the job.
   * @return true if an attempt was made, false if attempts ran out
   */
  public boolean run() {
    while (mRetryPolicy.attempt()) {
      mJobId = null;
      try {
        mJobId = mClient.run(getJobConfig());
      } catch (IOException e) {
        int retryCount = mRetryPolicy.getAttemptCount();
        System.out.println(String.format("Retry %d Failed to start job with error: %s",
            retryCount, e.getMessage()));
        LOG.warn("Retry {} Failed to get status for job (jobId={}) {}", retryCount, mJobId, e);
        continue;
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
      jobInfo = mClient.getJobStatusDetailed(mJobId);
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
        mFailedTasks = jobInfo.getChildren().stream()
            .filter(child -> child.getStatus() == Status.FAILED).collect(Collectors.toSet());
        setFailedFiles();
      } else if (jobInfo.getStatus().equals(Status.COMPLETED)) {
        logCompleted();
      }
      return jobInfo.getStatus();
    }
    return Status.RUNNING;
  }

  /**
   * Get job config.
   * @return job config
   */
  public abstract JobConfig getJobConfig();

  /**
   * Get how many files contained in job attempt.
   * @return number of files
   */
  public abstract int getSize();

  /**
   * Get failed files if there's any. Only call this function after confirm job status is FAILED!
   * @return failed fail set
   */
  public Set<String> getFailedFiles() {
    return mFailedFiles;
  }

  protected abstract void setFailedFiles();

  protected abstract void logFailedAttempt(JobInfo jobInfo);

  protected abstract void logFailed();

  protected abstract void logCompleted();
}
