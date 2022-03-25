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

package alluxio.master.job.tracker;

import alluxio.exception.JobDoesNotExistException;
import alluxio.job.JobConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.master.job.JobMaster;
import alluxio.retry.RetryPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *  class for handling submission for a child job within a distributed command.
 */
public class CmdRunAttempt {
  private static final Logger LOG = LoggerFactory.getLogger(CmdRunAttempt.class);
//  static final long NON_EXISTED_JOB_ID = -1L;

  protected final RetryPolicy mRetryPolicy;
  protected final JobMaster mJobMaster;

  private Long mJobId;
  private long mCreationTime;

  private JobConfig mJobConfig;
  private long mFileCount;
  private long mFileSize;

  protected CmdRunAttempt(RetryPolicy retryPolicy, JobMaster jobMaster) {
    mRetryPolicy = retryPolicy;
    mJobMaster = jobMaster;
    mJobId = null;
  }

  /**
   * Set job config.
   * @param config
   */
  public void setConfig(JobConfig config) {
    mJobConfig = config;
  }

  /**
   * Set file count.
   * @param fileCount
   */
  public void setFileCount(long fileCount) {
    mFileCount = fileCount;
  }

  /**
   * Set file size.
   * @param fileSize
   */
  public void setFileSize(long fileSize) {
    mFileSize = fileSize;
  }

  /**
   * Get job config.
   * @return job config
   */
  public JobConfig getJobConfig() {
    return mJobConfig;
  }

  /**
   * Get file count.
   * @return file count
   */
  public long getFileCount() {
    return mFileCount;
  }

  /**
   * Get file size.
   * @return file size
   */
  public long getFileSize() {
    return mFileSize;
  }

  /**
   * Returns the job creation time.
   * @return creation time of a job
   */
  public long getCreationTime() {
    return mCreationTime;
  }

  /**
   * Runs the job.
   * @return true if an attempt was made, false if attempts ran out
   */
  public boolean run() {
    mCreationTime = System.currentTimeMillis(); // set job creation time.
    //System.out.println(String.format("creation time is %d", mCreationTime));
    while (mRetryPolicy.attempt()) {
      try {
        mJobId = mJobMaster.run(getJobConfig());
        //System.out.println(String.format("after: In CmdRunAttept, start job %d", mJobId));
      } catch (IOException | JobDoesNotExistException e) {
        int retryCount = mRetryPolicy.getAttemptCount();
        System.out.println(String.format("Retry %d Failed to start job with error: %s",
                retryCount, e.getMessage()));
        LOG.warn("Retry {} Failed to get status for job (jobId={}) {}", retryCount, mJobId, e);
        continue;
        // Do nothing. This will be counted as a failed attempt
      }
      return true;
    }
    //logFailed();
    return false;
  }

  /**
   * Returns the job Id.
   * @return job Id
   */
  public Long getJobId() {
    return mJobId;
  }

  /**
   * Returns the status of the job.
   * @return True if finished successfully or cancelled, False if FAILED and should be retried,
   *              null if the status should be checked again later
   */
  public Status checkJobStatus() {
    if (mJobId == null) {
      return Status.FAILED;
    }

    JobInfo jobInfo;
    try {
      jobInfo = mJobMaster.getStatus(mJobId);
    } catch (JobDoesNotExistException e) {
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
      return jobInfo.getStatus();
    }
    return Status.RUNNING;
  }

//  protected abstract void logFailedAttempt(JobInfo jobInfo);
//
//  protected abstract void logFailed();
//
//  protected abstract void logCompleted();
}
