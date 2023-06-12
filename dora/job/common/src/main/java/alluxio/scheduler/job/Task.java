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

package alluxio.scheduler.job;

import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.wire.WorkerInfo;

import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * A task that can be executed on a worker. Belongs to a {@link Job}.
 *
 * @param <V> the response type of the task
 */
public abstract class Task<V> implements Comparable<Task> {

  /**
   * Metrics and stats to track current task.
   */
  public static class TaskStat {
    private final Stopwatch mStopwatch = Stopwatch.createStarted();
    private long mTimeInQ = -1L;
    private long mTotalTimeToComplete = -1L;

    /**
     * Record time when task is inside the queue.
     */
    public void recordTimeInQ() {
      mTimeInQ = mStopwatch.elapsed(TimeUnit.MILLISECONDS);
    }

    /**
     * Record time taken to complete the task.
     */
    public void recordTimeToComplete() {
      mTotalTimeToComplete = mStopwatch.elapsed(TimeUnit.MILLISECONDS);
    }

    /**
     * @return task state
     */
    public String dumpStats() {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("TimeInQ:%s%n", mTimeInQ == -1 ? "N/A" : mTimeInQ))
          .append(String.format("TotalTimeToComplete:%s%n",
              mTotalTimeToComplete == -1 ? "N/A" : mTimeInQ));
      return sb.toString();
    }
  }

  /**
   * constructs Task.
   * @param job
   * @param taskId
   */
  public Task(Job job, int taskId) {
    setJob(job);
    mTaskId = taskId;
    mTaskStat = new TaskStat();
  }

  /**
   * run the task.
   */
  protected abstract ListenableFuture<V> run(BlockWorkerClient client);

  private ListenableFuture<V> mResponseFuture;
  private TaskStat mTaskStat;
  private int mPriority = 1;
  private int mTaskId;
  private WorkerInfo mMyWorker;
  protected Job mMyJob;

  /**
   * Get the worker info this task runs on.
   * @return my running worker
   */
  public WorkerInfo getMyRunningWorker() {
    return mMyWorker;
  }

  /**
   * Set the worker info this task runs on.
   * @param workerInfo
   */
  public void setMyRunningWorker(WorkerInfo workerInfo) {
    mMyWorker = workerInfo;
  }

  /**
   * Get task id.
   * @return taskId
   */
  public int getTaskId() {
    return mTaskId;
  }

  /**
   * @return the response future
   */
  public ListenableFuture<V> getResponseFuture() {
    return mResponseFuture;
  }

  /**
   * run the task and set the response future.
   * @param client worker client
   * @param workerInfo the worker information
   */
  public void execute(BlockWorkerClient client, WorkerInfo workerInfo) {
    mMyWorker = workerInfo;
    mResponseFuture = run(client);
  }

  /**
   * Set the job.
   * @param job the job
   */
  public void setJob(Job job) {
    mMyJob = job;
  }

  /**
   * Get the job this task belongs to.
   * @return Job
   */
  public Job getJob() {
    return mMyJob;
  }

  /**
   * @return task stat
   */
  public TaskStat getTaskStat() {
    return mTaskStat;
  }

  /**
   * @return priority
   */
  public int getPriority() {
    return mPriority;
  }

  /**
   * Set priority.
   * @param priority
   */
  public void setPriority(int priority) {
    mPriority = priority;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Task<?> other = (Task<?>) obj;
    return getTaskId() == other.getTaskId() && Objects.equals(mMyJob, other.mMyJob);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mPriority, mMyJob);
  }

  @Override
  public int compareTo(Task o) {
    if (this == o) {
      return 0;
    } else if (o == null) {
      return 1;
    }
    return getPriority() - o.getPriority();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("taskJobType", mMyJob.getClass())
        .add("taskJobId", mMyJob.getJobId())
        .add("taskId", mTaskId)
        .toString();
  }
}
