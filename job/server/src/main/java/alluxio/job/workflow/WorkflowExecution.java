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

package alluxio.job.workflow;

import alluxio.job.JobConfig;
import alluxio.job.wire.Status;
import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * Execution details of a workflow.
 */
public abstract class WorkflowExecution {

  private Status mStatus;
  private long mLastUpdated;
  private String mErrorType;
  private String mErrorMessage;

  /**
   * Default constructor.
   */
  public WorkflowExecution() {
    setStatus(Status.RUNNING);
    mErrorType = null;
    mErrorMessage = null;
  }

  /**
   * @return name of the workflow
   */
  public abstract String getName();

  /**
   * Given the previous set of jobs were completed successfully,
   * returns a list of jobs to execute next. Updating status accordingly.
   * @return list of {@link JobConfig} to execute next, empty when there is no more work
   */
  public final Set<JobConfig> next() {
    Preconditions.checkArgument(!mStatus.isFinished());
    Set<JobConfig> jobConfigs = nextJobs();
    if (jobConfigs.isEmpty()) {
      setStatus(Status.COMPLETED);
    }
    mLastUpdated = CommonUtils.getCurrentMs();
    return jobConfigs;
  }

  /**
   * stops future execution.
   * @param status status of the failure: either CANCELLED or FAILED
   * @param errorType error type
   * @param errorMessage error message
   */
  public final void stop(Status status, String errorType, String errorMessage) {
    Preconditions.checkArgument(status.equals(Status.CANCELED) || status.equals(Status.FAILED));
    setStatus(status);
    if (mErrorMessage == null) {
      mErrorType = errorType;
      mErrorMessage = errorMessage;
    }
  }

  /**
   * @return status of the workflow
   */
  public final Status getStatus() {
    return mStatus;
  }

  /**
   * @return the last time status was updated in milliseconds
   */
  public final long getLastUpdated() {
    return mLastUpdated;
  }

  /**
   * @return the error type
   */
  @Nullable
  public final String getErrorType() {
    return mErrorType;
  }

  /**
   * @return the error message
   */
  @Nullable
  public final String getErrorMessage() {
    return mErrorMessage;
  }

  private void setStatus(Status status) {
    mStatus = status;
    mLastUpdated = CommonUtils.getCurrentMs();
  }

  /**
   * Given the previous set of jobs were completed successfully,
   * returns a list of jobs to execute next.
   * @return list of {@link JobConfig} to execute next, empty when there is no more work
   */
  protected abstract Set<JobConfig> nextJobs();
}
