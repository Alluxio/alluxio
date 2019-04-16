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

package alluxio.worker.job.task;

import alluxio.collections.Pair;
import alluxio.grpc.Status;
import alluxio.grpc.TaskInfo;
import alluxio.job.JobConfig;
import alluxio.job.RunTaskContext;
import alluxio.job.util.SerializationUtils;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Manages the task executors.
 */
@ThreadSafe
public class TaskExecutorManager {
  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorManager.class);

  private static final int DEFAULT_TASK_EXECUTOR_POOL_SIZE = 10;
  private final ExecutorService mTaskExecutionService = Executors.newFixedThreadPool(
      DEFAULT_TASK_EXECUTOR_POOL_SIZE, ThreadFactoryUtils.build("task-execution-service-%d", true));

  // These maps are all indexed by <Job ID, Task ID> pairs.
  /** Stores the futures for all running tasks. */
  private Map<Pair<Long, Integer>, Future<?>> mTaskFutures;
  /** Stores the task info for all running tasks. */
  private Map<Pair<Long, Integer>, TaskInfo.Builder> mUnfinishedTasks;
  /** Stores the updated tasks since the last call to {@link #getAndClearTaskUpdates()}. */
  private Map<Pair<Long, Integer>, TaskInfo> mTaskUpdates;

  /**
   *  Constructs a new instance of {@link TaskExecutorManager}.
   */
  public TaskExecutorManager() {
    mTaskFutures = Maps.newHashMap();
    mUnfinishedTasks = Maps.newHashMap();
    mTaskUpdates = Maps.newHashMap();
  }

  /**
   * Notifies the completion of the task.
   *
   * @param jobId the job id
   * @param taskId the task id
   * @param result the task execution result
   */
  public synchronized void notifyTaskCompletion(long jobId, int taskId, Object result) {
    Pair<Long, Integer> id = new Pair<>(jobId, taskId);
    TaskInfo.Builder taskInfo = mUnfinishedTasks.get(id);
    taskInfo.setStatus(Status.COMPLETED);
    try {
      taskInfo.setResult(ByteString.copyFrom(SerializationUtils.serialize(result)));
    } catch (IOException e) {
      // TODO(yupeng) better error handling
      LOG.warn("Failed to serialize {} : {}", result, e.getMessage());
      LOG.debug("Exception: ", e);
    } finally {
      finishTask(id);
      LOG.info("Task {} for job {} completed.", taskId, jobId);
    }
  }

  /**
   * Notifies the failure of the task.
   *
   * @param jobId the job id
   * @param taskId the task id
   * @param errorMessage the error message
   */
  public synchronized void notifyTaskFailure(long jobId, int taskId, String errorMessage) {
    Pair<Long, Integer> id = new Pair<>(jobId, taskId);
    TaskInfo.Builder taskInfo = mUnfinishedTasks.get(id);
    taskInfo.setStatus(Status.FAILED);
    taskInfo.setErrorMessage(errorMessage);
    finishTask(id);
    LOG.info("Task {} for job {} failed: {}", taskId, jobId, errorMessage);
  }

  /**
   * Notifies the cancellation of the task.
   *
   * @param jobId the job id
   * @param taskId the task id
   */
  public synchronized void notifyTaskCancellation(long jobId, int taskId) {
    Pair<Long, Integer> id = new Pair<>(jobId, taskId);
    TaskInfo.Builder taskInfo = mUnfinishedTasks.get(id);
    taskInfo.setStatus(Status.CANCELED);
    finishTask(id);
    LOG.info("Task {} for job {} canceled", taskId, jobId);
  }

  /**
   * Executes the given task.
   *
   * @param jobId the job id
   * @param taskId the task id
   * @param jobConfig the job configuration
   * @param taskArgs the arguments
   * @param context the context of the worker
   */
  public synchronized void executeTask(long jobId, int taskId, JobConfig jobConfig,
      Serializable taskArgs, RunTaskContext context) {
    Future<?> future = mTaskExecutionService
        .submit(new TaskExecutor(jobId, taskId, jobConfig, taskArgs, context, this));
    Pair<Long, Integer> id = new Pair<>(jobId, taskId);
    mTaskFutures.put(id, future);
    TaskInfo.Builder taskInfo = TaskInfo.newBuilder();
    taskInfo.setJobId(jobId);
    taskInfo.setTaskId(taskId);
    taskInfo.setStatus(Status.RUNNING);
    mUnfinishedTasks.put(id, taskInfo);
    mTaskUpdates.put(id, taskInfo.build());
    LOG.info("Task {} for job {} started", taskId, jobId);
  }

  /**
   * Cancels the given task.
   *
   * @param jobId the job id
   * @param taskId the task id
   */
  public synchronized void cancelTask(long jobId, int taskId) {
    Pair<Long, Integer> id = new Pair<>(jobId, taskId);
    TaskInfo.Builder taskInfo = mUnfinishedTasks.get(id);
    if (!mTaskFutures.containsKey(id) || taskInfo.getStatus().equals(Status.CANCELED)) {
      // job has finished, or failed, or canceled
      return;
    }

    Future<?> future = mTaskFutures.get(id);
    if (!future.cancel(true)) {
      taskInfo.setStatus(Status.FAILED);
      taskInfo.setErrorMessage("Failed to cancel the task");
      finishTask(id);
    }
  }

  /**
   * @return the list of task information
   */
  public synchronized List<TaskInfo> getAndClearTaskUpdates() {
    try {
      return ImmutableList.<TaskInfo>copyOf(mTaskUpdates.values());
    } finally {
      mTaskUpdates.clear();
    }
  }

  /**
   * Adds the given tasks to the task updates data structure. If there is already an update for the
   * specified task, it is not changed.
   *
   * @param tasks the tasks to restore
   */
  public synchronized void restoreTaskUpdates(List<TaskInfo> tasks) {
    for (TaskInfo task : tasks) {
      Pair<Long, Integer> id = new Pair<>(task.getJobId(), task.getTaskId());
      if (!mTaskUpdates.containsKey(id)) {
        mTaskUpdates.put(id, task);
      }
    }
  }

  private void finishTask(Pair<Long, Integer> id) {
    TaskInfo.Builder taskInfo = mUnfinishedTasks.get(id);
    mTaskFutures.remove(id);
    mUnfinishedTasks.remove(id);
    mTaskUpdates.put(id, taskInfo.build());
  }
}
