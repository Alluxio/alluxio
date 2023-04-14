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

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * A task that can be executed on a worker. Belongs to a {@link Job}.
 *
 * @param <V> the response type of the task
 */
public abstract class Task<V> implements Comparable<Task> {

  public static class TaskStat {
    private final Stopwatch mStopwatch = Stopwatch.createStarted();
    private long mTimeInQ = -1L;
    private long mTotalTimeToComplete = -1L;

    public void recordTimeInQ() {
      mTimeInQ = mStopwatch.elapsed(TimeUnit.MILLISECONDS);
    }

    public void recordTimeToComplete() {
      mTotalTimeToComplete = mStopwatch.elapsed(TimeUnit.MILLISECONDS);
    }

    public String dumpStats() {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("TimeInQ:%s\n", mTimeInQ == -1 ? "N/A" : mTimeInQ))
          .append(String.format("TotalTimeToComplete:%s\n", mTotalTimeToComplete == -1 ? "N/A" : mTimeInQ));
      return sb.toString();
    }
  }

  public Task () {
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
   */
  public void execute(BlockWorkerClient client, WorkerInfo workerInfo) {
    mRunsOnWorker = workerInfo;
    mResponseFuture = run(client);
  }

  public Task withJob(Job job) {
    mMyJob = job;
    return this;
  }

  public TaskStat getTaskStat() {
    return mTaskStat;
  }

  public int getPriority() {
    return mPriority;
  }

  @Override
  public int compareTo(Task anotherTask) {
    return mPriority - anotherTask.getPriority();
  }

  public void onComplete(Executor executor) {}
}
