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
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.CommonUtils;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;

import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

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
    if (mStatus != status) {
      if (mStatus != null) {
        Metrics.counter(mStatus).dec();
      }
      Metrics.counter(status).inc();
    }
    mStatus = status;
    mLastUpdated = CommonUtils.getCurrentMs();
  }

  /**
   * Given the previous set of jobs were completed successfully,
   * returns a list of jobs to execute next.
   * @return list of {@link JobConfig} to execute next, empty when there is no more work
   */
  protected abstract Set<JobConfig> nextJobs();

  @ThreadSafe
  private static final class Metrics {
    // Note that only counter/guage can be added here.
    // Both meter and timer need to be used inline
    // because new meter and timer will be created after {@link MetricsSystem.resetAllMetrics()}
    private static final Counter JOB_CANCELED =
        MetricsSystem.counter(MetricKey.MASTER_JOB_CANCELED.getName());
    private static final Counter JOB_COMPLETED =
        MetricsSystem.counter(MetricKey.MASTER_JOB_COMPLETED.getName());
    private static final Counter JOB_CREATED =
        MetricsSystem.counter(MetricKey.MASTER_JOB_CREATED.getName());
    private static final Counter JOB_FAILED =
        MetricsSystem.counter(MetricKey.MASTER_JOB_FAILED.getName());
    private static final Counter JOB_RUNNING =
        MetricsSystem.counter(MetricKey.MASTER_JOB_RUNNING.getName());

    private Metrics() {} // prevent instantiation

    private static Counter counter(Status status) {
      switch (status) {
        case CANCELED:
          return JOB_CANCELED;
        case COMPLETED:
          return JOB_COMPLETED;
        case CREATED:
          return JOB_CREATED;
        case FAILED:
          return JOB_FAILED;
        default:
          return JOB_RUNNING;
      }
    }
  }
}
