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

package alluxio.master.job;

import static java.util.Objects.requireNonNull;

import alluxio.master.scheduler.Scheduler;
import alluxio.scheduler.job.Job;
import alluxio.scheduler.job.JobState;
import alluxio.scheduler.job.Task;

import org.eclipse.jetty.util.BlockingArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract class for job. It provides basic job information and state management.
 *
 * @param <T> the type of the task of the job
 */
public abstract class AbstractJob<T extends Task<?>> implements Job<T> {
  private static final Logger LOG = LoggerFactory.getLogger(LoadJob.class);
  protected final String mJobId;
  protected final AtomicInteger mTaskIdGenerator = new AtomicInteger(0);
  protected JobState mState; // TODO(lucy) make it thread safe state update
  protected OptionalLong mEndTime = OptionalLong.empty();
  protected final long mStartTime;
  protected final Optional<String> mUser;
  protected final BlockingArrayQueue<Task<T>> mTaskList = new BlockingArrayQueue<>();
  protected WorkerAssignPolicy mWorkerAssignPolicy;

  /**
   * Creates a new instance of {@link AbstractJob}.
   *
   * @param user the user who submitted the job
   * @param jobId the job id
   */
  public AbstractJob(Optional<String> user, String jobId) {
    this(user, jobId, new HashBasedWorkerAssignPolicy());
  }

  /**
   * Creates a new instance of {@link AbstractJob}.
   * @param user
   * @param jobId
   * @param workerAssignPolicy
   */
  public AbstractJob(Optional<String> user, String jobId, WorkerAssignPolicy workerAssignPolicy) {
    mUser = requireNonNull(user, "user is null");
    mJobId = requireNonNull(jobId, "jobId is null");
    mState = JobState.RUNNING;
    mStartTime = System.currentTimeMillis();
    mWorkerAssignPolicy = workerAssignPolicy;
  }

  /**
   * Sets the worker assign policy.
   * @param assignPolicy the assign policy
   */
  public void setWorkerAssignPolicy(WorkerAssignPolicy assignPolicy) {
    mWorkerAssignPolicy = assignPolicy;
  }

  /**
   * Gets the worker assign policy.
   * @return assignPolicy the assign policy
   */
  public WorkerAssignPolicy getWorkerAssignPolicy() {
    return mWorkerAssignPolicy;
  }

  @Override
  public String getJobId() {
    return mJobId;
  }

  /**
   * Get end time.
   *
   * @return end time
   */
  @Override
  public OptionalLong getEndTime() {
    return mEndTime;
  }

  /**
   * Update end time.
   *
   * @param time time in ms
   */
  public void setEndTime(long time) {
    mEndTime = OptionalLong.of(time);
  }

  /**
   * Get load status.
   *
   * @return the load job's status
   */
  @Override
  public JobState getJobState() {
    return mState;
  }

  /**
   * Set load state.
   *
   * @param state new state
   * @param journalUpdate true if state change needs to be journaled
   */
  @Override
  public void setJobState(JobState state, boolean journalUpdate) {
    LOG.debug("Change JobState to {} for job {}, journalUpdate:{}", state, this, journalUpdate);
    mState = state;
    if (!isRunning()) {
      mEndTime = OptionalLong.of(System.currentTimeMillis());
    }
    if (journalUpdate) {
      Scheduler.getInstance().getJobMetaStore().updateJob(this);
    }
  }

  @Override
  public boolean isRunning() {
    return mState == JobState.RUNNING || mState == JobState.VERIFYING;
  }

  @Override
  public boolean isDone() {
    return mState == JobState.SUCCEEDED || mState == JobState.FAILED;
  }

  @Override
  public void initializeJob() {
    LOG.info("Job:{} initializing...", mJobId);
  }
}
