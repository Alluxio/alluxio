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

import alluxio.grpc.RunTaskCommand;
import alluxio.job.JobConfig;
import alluxio.job.plan.PlanDefinition;
import alluxio.job.plan.PlanDefinitionRegistry;
import alluxio.exception.JobDoesNotExistException;
import alluxio.job.RunTaskContext;
import alluxio.job.util.SerializationUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A thread that runs the task.
 */
@NotThreadSafe
public final class TaskExecutor implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class);

  private final long mJobId;
  private final long mTaskId;
  private final RunTaskCommand mRunTaskCommand;
  private final RunTaskContext mContext;
  private final TaskExecutorManager mTaskExecutorManager;

  /**
   * Creates a new instance of {@link TaskExecutor}.
   *
   * @param jobId the job id
   * @param taskId the task id
   * @param runTaskCommand the run task command
   * @param context the context on the worker
   * @param taskExecutorManager the task executor manager
   */
  public TaskExecutor(long jobId, long taskId, RunTaskCommand runTaskCommand,
      RunTaskContext context, TaskExecutorManager taskExecutorManager) {
    mJobId = jobId;
    mTaskId = taskId;
    mRunTaskCommand = runTaskCommand;
    mContext = Preconditions.checkNotNull(context);
    mTaskExecutorManager = Preconditions.checkNotNull(taskExecutorManager);
  }

  @Override
  public void run() {
    JobConfig jobConfig = null;
    Serializable taskArgs = null;
    try {
      jobConfig = (JobConfig) SerializationUtils.deserialize(
          mRunTaskCommand.getJobConfig().toByteArray());
      if (mRunTaskCommand.hasTaskArgs()) {
        taskArgs = SerializationUtils.deserialize(mRunTaskCommand.getTaskArgs().toByteArray());
      }
    } catch (IOException | ClassNotFoundException e) {
      fail(e, jobConfig, null);
    }

    PlanDefinition<JobConfig, Serializable, Serializable> definition;
    try {
      definition = PlanDefinitionRegistry.INSTANCE.getJobDefinition(jobConfig);
    } catch (JobDoesNotExistException e) {
      LOG.error("The job definition for config {} does not exist.", jobConfig.getName());
      fail(e, jobConfig, taskArgs);
      return;
    }

    mTaskExecutorManager.notifyTaskRunning(mJobId, mTaskId);
    Serializable result;
    try {
      result = definition.runTask(jobConfig, taskArgs, mContext);
    } catch (InterruptedException e) {
      // Cleanup around the interruption should already have been handled by a different thread
      Thread.currentThread().interrupt();
      return;
    } catch (Throwable t) {
      fail(t, jobConfig, taskArgs);
      return;
    }
    mTaskExecutorManager.notifyTaskCompletion(mJobId, mTaskId, result);
  }

  private void fail(Throwable t, JobConfig jobConfig, Serializable taskArgs) {
    mTaskExecutorManager.notifyTaskFailure(mJobId, mTaskId, t);

    LOG.warn("Exception running task for job {}({}) : {}",
        (jobConfig == null) ? "Undefined" : jobConfig.getName(),
        (taskArgs == null) ? "Undefined" : taskArgs.toString(), t.getMessage());
    return;
  }
}
