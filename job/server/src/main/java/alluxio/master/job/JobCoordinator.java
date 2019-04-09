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

import alluxio.job.JobConfig;
import alluxio.job.JobDefinition;
import alluxio.job.JobDefinitionRegistry;
import alluxio.job.JobServerContext;
import alluxio.exception.JobDoesNotExistException;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.meta.JobInfo;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.master.job.command.CommandManager;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A job coordinator that coordinates the distributed task execution on the worker nodes.
 */
@ThreadSafe
public final class JobCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(JobCoordinator.class);
  /**
   * Access to mJobInfo should be synchronized in order to avoid inconsistent internal task state.
   */
  private final JobInfo mJobInfo;
  private final CommandManager mCommandManager;

  /** The context containing containing the necessary references to schedule jobs. */
  private final JobServerContext mJobServerContext;

  /**
   * List of all job workers at the time when the job was started. If this coordinator was created
   * to represent an already-completed job, this list will be empty.
   */
  private final List<WorkerInfo> mWorkersInfoList;
  /**
   * Map containing the worker info for every task associated with the coordinated job. If this
   * coordinator was created to represent an already-completed job, this map will be empty.
   */
  private final Map<Integer, WorkerInfo> mTaskIdToWorkerInfo = Maps.newHashMap();
  /**
   * Mapping from workers running tasks for this job to the ids of those tasks. If this coordinator
   * was created to represent an already-completed job, this map will be empty.
   */
  private final Map<Long, Integer> mWorkerIdToTaskId = Maps.newHashMap();

  private JobCoordinator(CommandManager commandManager, JobServerContext jobServerContext,
      List<WorkerInfo> workerInfoList, Long jobId, JobConfig jobConfig,
      Function<JobInfo, Void> statusChangeCallback) {
    Preconditions.checkNotNull(jobConfig);
    mJobServerContext = jobServerContext;
    mJobInfo = new JobInfo(jobId, jobConfig, statusChangeCallback);
    mCommandManager = commandManager;
    mWorkersInfoList = workerInfoList;
  }

  /**
   * Creates a new instance of the {@link JobCoordinator}.
   *
   * @param commandManager the command manager
   * @param jobServerContext the context with information used to select executors
   * @param workerInfoList the list of workers to use
   * @param jobId the job Id
   * @param jobConfig configuration for the job
   * @param statusChangeCallback Callback to be called for status changes on the job
   * @return the created coordinator
   * @throws JobDoesNotExistException when the job definition doesn't exist
   */
  public static JobCoordinator create(CommandManager commandManager,
      JobServerContext jobServerContext, List<WorkerInfo> workerInfoList, Long jobId,
      JobConfig jobConfig, Function<JobInfo, Void> statusChangeCallback)
      throws JobDoesNotExistException {
    Preconditions.checkNotNull(commandManager, "commandManager");
    JobCoordinator jobCoordinator = new JobCoordinator(commandManager, jobServerContext,
        workerInfoList, jobId, jobConfig, statusChangeCallback);
    jobCoordinator.start();
    // start the coordinator, create the tasks
    return jobCoordinator;
  }

  private synchronized void start() throws JobDoesNotExistException {
    // get the job definition
    LOG.info("Starting job {}", mJobInfo.getJobConfig());
    JobDefinition<JobConfig, ?, ?> definition =
        JobDefinitionRegistry.INSTANCE.getJobDefinition(mJobInfo.getJobConfig());
    SelectExecutorsContext context =
        new SelectExecutorsContext(mJobInfo.getId(), mJobServerContext);
    Map<WorkerInfo, ?> taskAddressToArgs;
    try {
      taskAddressToArgs =
          definition.selectExecutors(mJobInfo.getJobConfig(), mWorkersInfoList, context);
    } catch (Exception e) {
      LOG.warn("Failed to select executor. {})", e.getMessage());
      LOG.debug("Exception: ", e);
      mJobInfo.setStatus(Status.FAILED);
      mJobInfo.setErrorMessage(e.getMessage());
      return;
    }
    if (taskAddressToArgs.isEmpty()) {
      LOG.warn("No executor was selected.");
      updateStatus();
    }

    for (Entry<WorkerInfo, ?> entry : taskAddressToArgs.entrySet()) {
      LOG.debug("Selected executor {} with parameters {}.", entry.getKey(), entry.getValue());
      int taskId = mTaskIdToWorkerInfo.size();
      // create task
      mJobInfo.addTask(taskId);
      // submit commands
      mCommandManager.submitRunTaskCommand(mJobInfo.getId(), taskId, mJobInfo.getJobConfig(),
          entry.getValue(), entry.getKey().getId());
      mTaskIdToWorkerInfo.put(taskId, entry.getKey());
      mWorkerIdToTaskId.put(entry.getKey().getId(), taskId);
    }
  }

  /**
   * Cancels the current job.
   */
  public synchronized void cancel() {
    for (int taskId : mJobInfo.getTaskIdList()) {
      mCommandManager.submitCancelTaskCommand(mJobInfo.getId(), taskId,
          mTaskIdToWorkerInfo.get(taskId).getId());
    }
  }

  /**
   * Updates internal status with given tasks.
   *
   * @param taskInfoList List of @TaskInfo instances to update
   */
  public synchronized void updateTasks(List<TaskInfo> taskInfoList) {
    for (TaskInfo taskInfo : taskInfoList) {
      mJobInfo.setTaskInfo(taskInfo.getTaskId(), taskInfo);
    }
    updateStatus();
  }

  /**
   * @return true if the job is finished
   */
  public synchronized boolean isJobFinished() {
    return mJobInfo.getStatus().isFinished();
  }

  /**
   * Sets the job as failed with given error message.
   *
   * @param errorMessage Error message to set for failure
   */
  public synchronized void setJobAsFailed(String errorMessage) {
    mJobInfo.setStatus(Status.FAILED);
    mJobInfo.setErrorMessage(errorMessage);
  }

  /**
   * Fails any incomplete tasks being run on the specified worker.
   *
   * @param workerId the id of the worker to fail tasks for
   */
  public synchronized void failTasksForWorker(long workerId) {
    Integer taskId = mWorkerIdToTaskId.get(workerId);
    if (taskId == null) {
      return;
    }
    TaskInfo taskInfo = mJobInfo.getTaskInfo(taskId);
    if (taskInfo.getStatus().isFinished()) {
      return;
    }
    taskInfo.setStatus(Status.FAILED);
    taskInfo.setErrorMessage("Job worker was lost before the task could complete");
    updateStatus();
  }

  /**
   * @return the on the wire job info for the job being coordinated
   */
  public synchronized alluxio.job.wire.JobInfo getJobInfoWire() {
    return new alluxio.job.wire.JobInfo(mJobInfo);
  }

  /**
   * Updates the status of the job. When all the tasks are completed, run the join method in the
   * definition.
   */
  private void updateStatus() {
    int completed = 0;
    List<TaskInfo> taskInfoList = mJobInfo.getTaskInfoList();
    for (TaskInfo info : taskInfoList) {
      switch (info.getStatus()) {
        case FAILED:
          mJobInfo.setStatus(Status.FAILED);
          if (mJobInfo.getErrorMessage().isEmpty()) {
            mJobInfo.setErrorMessage("Task execution failed: " + info.getErrorMessage());
          }
          return;
        case CANCELED:
          if (mJobInfo.getStatus() != Status.FAILED) {
            mJobInfo.setStatus(Status.CANCELED);
          }
          return;
        case RUNNING:
          if (mJobInfo.getStatus() != Status.FAILED && mJobInfo.getStatus() != Status.CANCELED) {
            mJobInfo.setStatus(Status.RUNNING);
          }
          break;
        case COMPLETED:
          completed++;
          break;
        case CREATED:
          // do nothing
          break;
        default:
          throw new IllegalArgumentException("Unsupported status " + info.getStatus());
      }
    }
    if (completed == taskInfoList.size()) {
      if (mJobInfo.getStatus() == Status.COMPLETED) {
        return;
      }

      // all the tasks completed, run join
      try {
        mJobInfo.setStatus(Status.COMPLETED);
        mJobInfo.setResult(join(taskInfoList));
      } catch (Exception e) {
        mJobInfo.setStatus(Status.FAILED);
        mJobInfo.setErrorMessage(e.getMessage());
      }
    }
  }

  /**
   * Joins the task results and produces a final result.
   *
   * @param taskInfoList the list of task information
   * @return the aggregated result as a String
   * @throws Exception if any error occurs
   */
  private String join(List<TaskInfo> taskInfoList) throws Exception {
    // get the job definition
    JobDefinition<JobConfig, Serializable, Serializable> definition =
        JobDefinitionRegistry.INSTANCE.getJobDefinition(mJobInfo.getJobConfig());
    Map<WorkerInfo, Serializable> taskResults = Maps.newHashMap();
    for (TaskInfo taskInfo : taskInfoList) {
      taskResults.put(mTaskIdToWorkerInfo.get(taskInfo.getTaskId()), taskInfo.getResult());
    }
    return definition.join(mJobInfo.getJobConfig(), taskResults);
  }
}
