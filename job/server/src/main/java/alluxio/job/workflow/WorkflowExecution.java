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
import alluxio.job.wire.WorkflowInfo;
import alluxio.util.CommonUtils;
import com.google.common.base.Preconditions;

import java.util.Set;

/**
 * TODO(bradley).
 *
 */
public abstract class WorkflowExecution {

  private Status mStatus;
  private long mLastUpdated;

  public WorkflowExecution() {
    setStatus(Status.RUNNING);
  }

  public final Set<JobConfig> next() {
    Preconditions.checkArgument(!mStatus.isFinished());
    Set<JobConfig> jobConfigs = nextJobs();
    if (jobConfigs.isEmpty()) {
      setStatus(Status.COMPLETED);
    }
    return jobConfigs;
  }

  public final boolean isDone() {
    return mStatus.equals(Status.COMPLETED);
  }

  public final void fail(Status status) {
    Preconditions.checkArgument(status.equals(Status.CANCELED) || status.equals(Status.FAILED));
    setStatus(status);
  }

  public final Status getStatus() {
    return mStatus;
  }

  public final long getLastUpdated() {
    return mLastUpdated;
  }

  private void setStatus(Status status) {
    mStatus = status;
    mLastUpdated = CommonUtils.getCurrentMs();
  }

  /**
   * @return list of {@link JobConfig} to run
   */
  protected abstract Set<JobConfig> nextJobs();

}
