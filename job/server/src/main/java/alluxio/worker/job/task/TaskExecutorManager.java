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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.RunTaskCommand;
import alluxio.job.ErrorUtils;
import alluxio.job.RunTaskContext;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.logging.SamplingLogger;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Manages the task executors.
 */
@ThreadSafe
public class TaskExecutorManager {
  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorManager.class);
  private static final SamplingLogger SAMPLING_LOGGER = new SamplingLogger(LOG, 30 * 1000);

  private static final int MAX_TASK_EXECUTOR_POOL_SIZE = 10000;

  private final PausableThreadPoolExecutor mTaskExecutionService;

  // These maps are all indexed by <Job ID, Task ID> pairs.
  /** Stores the futures for all running tasks. */
  private final Map<Pair<Long, Long>, Future<?>> mTaskFutures;
  /** Stores the task info for all running tasks. */
  private final Map<Pair<Long, Long>, TaskInfo> mUnfinishedTasks;
  /** Stores the updated tasks since the last call to {@link #getAndClearTaskUpdates()}. */
  private final Map<Pair<Long, Long>, TaskInfo> mTaskUpdates;

  private final WorkerNetAddress mAddress;

  private int mDefaultTaskExecutorPoolSize;
  private boolean mThrottled;

  /**
   * Constructs a new instance of {@link TaskExecutorManager}.
   * @param taskExecutorPoolSize number of task executors in the pool
   * @param address the worker address
   */
  public TaskExecutorManager(int taskExecutorPoolSize, WorkerNetAddress address) {
    mTaskFutures = Maps.newHashMap();
    mUnfinishedTasks = Maps.newHashMap();
    mTaskUpdates = Maps.newHashMap();
    mTaskExecutionService = new PausableThreadPoolExecutor(taskExecutorPoolSize,
        MAX_TASK_EXECUTOR_POOL_SIZE, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
        ThreadFactoryUtils.build("task-execution-service-%d", true));
    mDefaultTaskExecutorPoolSize = taskExecutorPoolSize;
    mThrottled = false;
    mAddress = address;
  }

  /**
   * @return number of active tasks
   */
  public int getNumActiveTasks() {
    return mTaskExecutionService.getNumActiveTasks();
  }

  /**
   * @return task executor pool size
   */
  public int getTaskExecutorPoolSize() {
    return mTaskExecutionService.getCorePoolSize();
  }

  /**
   * @param taskExecutorPoolSize number of threads in the task executor pool
   */
  private synchronized void setTaskExecutorPoolSize(int taskExecutorPoolSize) {
    Preconditions.checkArgument(taskExecutorPoolSize >= 0);
    Preconditions.checkArgument(taskExecutorPoolSize <= MAX_TASK_EXECUTOR_POOL_SIZE);

    if (taskExecutorPoolSize == 0) {
      // treat 0 as a special case because ThreadedTaskExecutorService can't seem to have 0 threads
      mTaskExecutionService.pause();
    } else {
      mTaskExecutionService.resume();
    }

    mTaskExecutionService.setCorePoolSize(taskExecutorPoolSize);
  }

  /**
   * Sets the number of threads in the task executor pool when not throttled.
   *
   * @param defaultTaskExecutorPoolSize number of threads in the task executor pool
   */
  public synchronized void setDefaultTaskExecutorPoolSize(int defaultTaskExecutorPoolSize) {
    mDefaultTaskExecutorPoolSize = defaultTaskExecutorPoolSize;
    if (!mThrottled) {
      setTaskExecutorPoolSize(mDefaultTaskExecutorPoolSize);
    }
  }

  /**
   * Throttle the TaskExecutorManager. This sets the internal thread pool size to 0 until
   * unthrottled.
   */
  public synchronized void throttle() {
    mThrottled = true;
    setTaskExecutorPoolSize(0);
  }

  /**
   * Unthrottle the TaskExecutorManager. This sets the internal thread pool size back to the
   * default thread pool size.
   */
  public synchronized void unthrottle() {
    mThrottled = false;
    setTaskExecutorPoolSize(mDefaultTaskExecutorPoolSize);
  }

  /**
   * @return number of unfinished tasks
   */
  public int unfinishedTasks() {
    return mUnfinishedTasks.size();
  }

  /**
   * Noitfy the start of the task.
   *
   * @param jobId the job id
   * @param taskId the task id
   */
  public synchronized void notifyTaskRunning(long jobId, long taskId) {
    Pair<Long, Long> id = new Pair<>(jobId, taskId);
    TaskInfo taskInfo = mUnfinishedTasks.get(id);
    taskInfo.setStatus(Status.RUNNING);
    LOG.info("Task {} for job {} started", taskId, jobId);
  }

  /**
   * Notifies the completion of the task.
   *
   * @param jobId the job id
   * @param taskId the task id
   * @param result the task execution result
   */
  public synchronized void notifyTaskCompletion(long jobId, long taskId, Serializable result) {
    Pair<Long, Long> id = new Pair<>(jobId, taskId);
    TaskInfo taskInfo = mUnfinishedTasks.get(id);
    taskInfo.setStatus(Status.COMPLETED);
    taskInfo.setResult(result);
    finishTask(id);
    LOG.info("Task {} for job {} completed.", taskId, jobId);
  }

  /**
   * Notifies the failure of the task.
   *
   * @param jobId the job id
   * @param taskId the task id
   * @param t the thrown exception
   */
  public synchronized void notifyTaskFailure(long jobId, long taskId, Throwable t) {
    Pair<Long, Long> id = new Pair<>(jobId, taskId);
    TaskInfo taskInfo = mUnfinishedTasks.get(id);
    taskInfo.setStatus(Status.FAILED);

    String errorMessage;
    if (ServerConfiguration.getBoolean(PropertyKey.DEBUG)) {
      errorMessage = Throwables.getStackTraceAsString(t);
    } else {
      errorMessage = t.getMessage();
    }

    taskInfo.setErrorType(ErrorUtils.getErrorType(t));
    if (errorMessage != null) {
      taskInfo.setErrorMessage(errorMessage);
    }
    finishTask(id);
    LOG.info("Task {} for job {} failed: {}", taskId, jobId, errorMessage);
    SAMPLING_LOGGER.info("Stack trace for taskId: {} jobId: {} : {}", taskId, jobId,
        Throwables.getStackTraceAsString(t));
  }

  /**
   * Executes the given task.
   *
   * @param jobId the job id
   * @param taskId the task id
   * @param runTaskCommand the run task command
   * @param context the context of the worker
   */
  public synchronized void executeTask(long jobId, long taskId, RunTaskCommand runTaskCommand,
      RunTaskContext context) {
    Future<?> future = mTaskExecutionService
        .submit(new TaskExecutor(jobId, taskId, runTaskCommand, context, this));
    Pair<Long, Long> id = new Pair<>(jobId, taskId);
    mTaskFutures.put(id, future);
    TaskInfo taskInfo = new TaskInfo(jobId, taskId, Status.CREATED, mAddress, null);

    mUnfinishedTasks.put(id, taskInfo);
    mTaskUpdates.put(id, taskInfo);
    LOG.info("Task {} for job {} received", taskId, jobId);
  }

  /**
   * Cancels the given task.
   *
   * @param jobId the job id
   * @param taskId the task id
   */
  public synchronized void cancelTask(long jobId, long taskId) {
    Pair<Long, Long> id = new Pair<>(jobId, taskId);
    TaskInfo taskInfo = mUnfinishedTasks.get(id);
    if (!mTaskFutures.containsKey(id) || taskInfo.getStatus().equals(Status.CANCELED)) {
      // job has finished, or failed, or canceled
      return;
    }

    LOG.info("Task {} for job {} canceled", taskId, jobId);
    Future<?> future = mTaskFutures.get(id);
    if (!future.cancel(true)) {
      taskInfo.setStatus(Status.FAILED);
      taskInfo.setErrorType("FailedCancel");
      taskInfo.setErrorMessage("Failed to cancel the task");
    } else {
      taskInfo.setStatus(Status.CANCELED);
    }
    finishTask(id);
  }

  /**
   * @return the list of task information
   */
  public synchronized List<TaskInfo> getAndClearTaskUpdates() {
    try {
      return ImmutableList.copyOf(mTaskUpdates.values());
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
      Pair<Long, Long> id = new Pair<>(task.getParentId(), task.getId());
      if (!mTaskUpdates.containsKey(id)) {
        mTaskUpdates.put(id, task);
      }
    }
  }

  private void finishTask(Pair<Long, Long> id) {
    TaskInfo taskInfo = mUnfinishedTasks.get(id);
    mTaskFutures.remove(id);
    mUnfinishedTasks.remove(id);
    mTaskUpdates.put(id, taskInfo);
  }
}
