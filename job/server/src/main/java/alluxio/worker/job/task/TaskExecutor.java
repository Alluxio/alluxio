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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.job.JobConfig;
import alluxio.job.JobDefinition;
import alluxio.job.JobDefinitionRegistry;
import alluxio.job.JobWorkerContext;
import alluxio.exception.JobDoesNotExistException;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A thread that runs the task.
 */
@NotThreadSafe
public final class TaskExecutor implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class);

  private final long mJobId;
  private final int mTaskId;
  private final JobConfig mJobConfig;
  private final Serializable mTaskArgs;
  private final JobWorkerContext mContext;
  private final TaskExecutorManager mTaskExecutorManager;

  /**
   * Creates a new instance of {@link TaskExecutor}.
   *
   * @param jobId the job id
   * @param taskId the task id
   * @param jobConfig the job configuration
   * @param taskArgs the arguments passed to the task
   * @param context the context on the worker
   * @param taskExecutorManager the task executor manager
   */
  public TaskExecutor(long jobId, int taskId, JobConfig jobConfig, Serializable taskArgs,
      JobWorkerContext context, TaskExecutorManager taskExecutorManager) {
    mJobId = jobId;
    mTaskId = taskId;
    mJobConfig = jobConfig;
    mTaskArgs = taskArgs;
    mContext = Preconditions.checkNotNull(context);
    mTaskExecutorManager = Preconditions.checkNotNull(taskExecutorManager);
  }

  @Override
  public void run() {
    // TODO(yupeng) set other logger
    JobDefinition<JobConfig, Serializable, Serializable> definition;
    try {
      definition = JobDefinitionRegistry.INSTANCE.getJobDefinition(mJobConfig);
    } catch (JobDoesNotExistException e) {
      LOG.error("The job definition for config {} does not exist.", mJobConfig.getName());
      return;
    }
    Object result;
    try {
      result = definition.runTask(mJobConfig, mTaskArgs, mContext);
      if (Thread.interrupted()) {
        mTaskExecutorManager.notifyTaskCancellation(mJobId, mTaskId);
      }
    } catch (InterruptedException e) {
      mTaskExecutorManager.notifyTaskCancellation(mJobId, mTaskId);
      return;
    } catch (Throwable t) {
      if (Configuration.getBoolean(PropertyKey.DEBUG)) {
        mTaskExecutorManager.notifyTaskFailure(mJobId, mTaskId, ExceptionUtils.getStackTrace(t));
      } else {
        mTaskExecutorManager.notifyTaskFailure(mJobId, mTaskId, t.getMessage());
      }
      LOG.warn("Exception running task for job {}({}) : {}", mJobConfig.getName(),
          mTaskArgs.toString(), t.getMessage());
      return;
    }
    mTaskExecutorManager.notifyTaskCompletion(mJobId, mTaskId, result);
  }
}
