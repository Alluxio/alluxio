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

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * A task that can be executed on a worker. Belongs to a {@link Job}.
 *
 * @param <V> the response type of the task
 */
public abstract class Task<V> implements Comparable<Task> {

  /**
   * State of a task.
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
   * Constructor.
   */
  public Task() {
    mTaskStat = new TaskStat();
  }

  /**
   * run the task.
   */
  protected abstract ListenableFuture<V> run(BlockWorkerClient client);

  private ListenableFuture<V> mResponseFuture;
  private TaskStat mTaskStat;
  private int mPriority = 1;
  private WorkerInfo mRunsOnWorker;
  protected Job mMyJob;

  /**
   * @return my running worker
   */
  public WorkerInfo getMyRunningWorker() {
    return mRunsOnWorker;
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
    mRunsOnWorker = workerInfo;
    mResponseFuture = run(client);
  }

  /**
   * @param job the job
   * @return the task
   */
  public Task withJob(Job job) {
    mMyJob = job;
    return this;
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

  @Override
  public boolean equals(Object obj) {
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Task<?> other = (Task<?>) obj;
    return getPriority() == other.getPriority()
        && getTaskStat() == other.getTaskStat()
        && Objects.equals(mRunsOnWorker, other.mRunsOnWorker)
        && Objects.equals(mMyJob, other.mMyJob)
        && Objects.equals(mResponseFuture, other.mResponseFuture);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mPriority, mTaskStat, mRunsOnWorker, mMyJob, mResponseFuture);
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

  /**
   * @param executor the executor
   */
  public void onComplete(Executor executor) {}
}
