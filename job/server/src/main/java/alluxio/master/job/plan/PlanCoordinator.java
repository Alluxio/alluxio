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

package alluxio.master.job.plan;

import alluxio.collections.Pair;
import alluxio.exception.JobDoesNotExistException;
import alluxio.job.ErrorUtils;
import alluxio.job.JobConfig;
import alluxio.job.plan.PlanDefinition;
import alluxio.job.plan.PlanDefinitionRegistry;
import alluxio.job.JobServerContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.plan.meta.PlanInfo;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.master.job.command.CommandManager;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A job coordinator that coordinates the distributed task execution on the worker nodes.
 */
@ThreadSafe
public final class PlanCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(PlanCoordinator.class);
  /**
   * Access to mJobInfo should be synchronized in order to avoid inconsistent internal task state.
   */
  private final PlanInfo mPlanInfo;
  private final CommandManager mCommandManager;

  /** The context containing containing the necessary references to schedule jobs. */
  private final JobServerContext mJobServerContext;

  /**
   * List of all job workers at the time when the job was started. If this coordinator was created
   * to represent an already-completed job, this list will be empty.
   */
  private List<WorkerInfo> mWorkersInfoList;
  /**
   * Map containing the worker info for every task associated with the coordinated job. If this
   * coordinator was created to represent an already-completed job, this map will be empty.
   */
  private final Map<Long, WorkerInfo> mTaskIdToWorkerInfo = Maps.newHashMap();
  /**
   * Mapping from workers running tasks for this job to the ids of those tasks. If this coordinator
   * was created to represent an already-completed job, this map will be empty.
   */
  private final Map<Long, List<Long>> mWorkerIdToTaskIds = Maps.newHashMap();

  private PlanCoordinator(CommandManager commandManager, JobServerContext jobServerContext,
                          List<WorkerInfo> workerInfoList, Long jobId, JobConfig jobConfig,
                          Consumer<PlanInfo> statusChangeCallback) {
    Preconditions.checkNotNull(jobConfig);
    mJobServerContext = jobServerContext;
    mPlanInfo = new PlanInfo(jobId, jobConfig, statusChangeCallback);
    mCommandManager = commandManager;
    mWorkersInfoList = workerInfoList;
  }

  /**
   * Creates a new instance of the {@link PlanCoordinator}.
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
  public static PlanCoordinator create(CommandManager commandManager,
      JobServerContext jobServerContext, List<WorkerInfo> workerInfoList, Long jobId,
      JobConfig jobConfig, Consumer<PlanInfo> statusChangeCallback)
      throws JobDoesNotExistException {
    Preconditions.checkNotNull(commandManager, "commandManager");
    PlanCoordinator planCoordinator = new PlanCoordinator(commandManager, jobServerContext,
        workerInfoList, jobId, jobConfig, statusChangeCallback);
    planCoordinator.start();
    // start the coordinator, create the tasks
    return planCoordinator;
  }

  private synchronized void start() throws JobDoesNotExistException {
    // get the job definition
    LOG.info("Starting job Id={} Config={}", mPlanInfo.getId(), mPlanInfo.getJobConfig());
    PlanDefinition<JobConfig, ?, ?> definition =
        PlanDefinitionRegistry.INSTANCE.getJobDefinition(mPlanInfo.getJobConfig());
    SelectExecutorsContext context =
        new SelectExecutorsContext(mPlanInfo.getId(), mJobServerContext);
    Set<? extends Pair<WorkerInfo, ?>> taskAddressToArgs;
    ArrayList<WorkerInfo> workersInfoListCopy = Lists.newArrayList(mWorkersInfoList);
    Collections.shuffle(workersInfoListCopy);
    try {
      taskAddressToArgs =
          definition.selectExecutors(mPlanInfo.getJobConfig(), workersInfoListCopy, context);
    } catch (Exception e) {
      LOG.warn("Failed to select executor. {})", e.toString());
      LOG.info("Exception: ", e);
      setJobAsFailed(ErrorUtils.getErrorType(e), e.getMessage());
      return;
    }
    if (taskAddressToArgs.isEmpty()) {
      LOG.warn("No executor was selected.");
      updateStatus();
    }

    for (Pair<WorkerInfo, ?> pair : taskAddressToArgs) {
      LOG.debug("Selected executor {} with parameters {}.", pair.getFirst(), pair.getSecond());
      int taskId = mTaskIdToWorkerInfo.size();
      // create task
      mPlanInfo.addTask(taskId, pair.getFirst(), pair.getSecond());
      // submit commands
      mCommandManager.submitRunTaskCommand(mPlanInfo.getId(), taskId, mPlanInfo.getJobConfig(),
          pair.getSecond(), pair.getFirst().getId());
      mTaskIdToWorkerInfo.put((long) taskId, pair.getFirst());
      mWorkerIdToTaskIds.putIfAbsent(pair.getFirst().getId(), Lists.newArrayList());
      mWorkerIdToTaskIds.get(pair.getFirst().getId()).add((long) taskId);
    }
  }

  /**
   * Cancels the current job.
   */
  public synchronized void cancel() {
    for (long taskId : mPlanInfo.getTaskIdList()) {
      mCommandManager.submitCancelTaskCommand(mPlanInfo.getId(), taskId,
          mTaskIdToWorkerInfo.get(taskId).getId());
    }
    mWorkersInfoList = null;
  }

  /**
   * Updates internal status with given tasks.
   *
   * @param taskInfoList List of @TaskInfo instances to update
   */
  public synchronized void updateTasks(List<TaskInfo> taskInfoList) {
    for (TaskInfo taskInfo : taskInfoList) {
      mPlanInfo.setTaskInfo(taskInfo.getTaskId(), taskInfo);
    }
    updateStatus();
    if (isJobFinished()) {
      mWorkersInfoList = null;
    }
  }

  /**
   * @return true if the job is finished
   */
  public synchronized boolean isJobFinished() {
    return mPlanInfo.getStatus().isFinished();
  }

  /**
   * @return the id corresponding to the job
   */
  public long getJobId() {
    return mPlanInfo.getId();
  }

  /**
   * Sets the job as failed with given error message.
   *
   * @param errorType Error type to set for failure
   * @param errorMessage Error message to set for failure
   */
  public synchronized void setJobAsFailed(String errorType, String errorMessage) {
    if (!mPlanInfo.getStatus().isFinished()) {
      mPlanInfo.setStatus(Status.FAILED);
      mPlanInfo.setErrorType(errorType);
      mPlanInfo.setErrorMessage(errorMessage);
    }
    mWorkersInfoList = null;
  }

  /**
   * Fails any incomplete tasks being run on the specified worker.
   *
   * @param workerId the id of the worker to fail tasks for
   */
  public void failTasksForWorker(long workerId) {
    synchronized (mPlanInfo) {
      if (mPlanInfo.getStatus().isFinished()) {
        return;
      }

      List<Long> taskIds = mWorkerIdToTaskIds.get(workerId);
      if (taskIds == null) {
        return;
      }

      boolean statusChanged = false;
      for (Long taskId : taskIds) {
        TaskInfo taskInfo = mPlanInfo.getTaskInfo(taskId);
        if (taskInfo == null || taskInfo.getStatus().isFinished()) {
          continue;
        }
        taskInfo.setStatus(Status.FAILED);
        taskInfo.setErrorType("JobWorkerLost");
        taskInfo.setErrorMessage(String.format("Job worker(%s) was lost before "
                + "the task(%d) could complete", taskInfo.getWorkerHost(), taskId));
        statusChanged = true;
        break;
      }
      if (statusChanged) {
        updateStatus();
      }
    }
  }

  /**
   * @param verbose whether the output should be verbose or not
   * @return the on the wire job info for the job being coordinated
   */
  public synchronized alluxio.job.wire.PlanInfo getPlanInfoWire(boolean verbose) {
    return new alluxio.job.wire.PlanInfo(mPlanInfo, verbose);
  }

  /**
   * Updates the status of the job. When all the tasks are completed, run the join method in the
   * definition.
   */
  private synchronized void updateStatus() {
    int completed = 0;
    List<TaskInfo> taskInfoList = mPlanInfo.getTaskInfoList();
    for (TaskInfo info : taskInfoList) {
      switch (info.getStatus()) {
        case FAILED:
          setJobAsFailed(info.getErrorType(), "Task execution failed: " + info.getErrorMessage());
          return;
        case CANCELED:
          if (mPlanInfo.getStatus() != Status.FAILED) {
            mPlanInfo.setStatus(Status.CANCELED);
          }
          return;
        case RUNNING:
          if (mPlanInfo.getStatus() != Status.FAILED && mPlanInfo.getStatus() != Status.CANCELED) {
            mPlanInfo.setStatus(Status.RUNNING);
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
      if (mPlanInfo.getStatus() == Status.COMPLETED) {
        return;
      }

      // all the tasks completed, run join
      try {
        // Try to join first, so that in case of failure we don't move to a completed state yet
        mPlanInfo.setResult(join(taskInfoList));
        mPlanInfo.setStatus(Status.COMPLETED);
      } catch (Exception e) {
        LOG.warn("Job error when joining tasks Job Id={} Config={}",
            mPlanInfo.getId(), mPlanInfo.getJobConfig(), e);
        setJobAsFailed(ErrorUtils.getErrorType(e), e.getMessage());
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
    PlanDefinition<JobConfig, Serializable, Serializable> definition =
        PlanDefinitionRegistry.INSTANCE.getJobDefinition(mPlanInfo.getJobConfig());
    Map<WorkerInfo, Serializable> taskResults = Maps.newHashMap();
    for (TaskInfo taskInfo : taskInfoList) {
      taskResults.put(mTaskIdToWorkerInfo.get(taskInfo.getTaskId()), taskInfo.getResult());
    }
    return definition.join(mPlanInfo.getJobConfig(), taskResults);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof PlanCoordinator)) {
      return false;
    }

    PlanCoordinator other = (PlanCoordinator) o;
    return Objects.equal(mPlanInfo, other.mPlanInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mPlanInfo);
  }
}
