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

import alluxio.job.JobConfig;
import alluxio.job.JobDefinition;
import alluxio.job.JobDefinitionRegistry;
import alluxio.job.JobServerContext;
import alluxio.job.RunTaskContext;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.Serializable;

/**
 * Tests {@link TaskExecutor}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({TaskExecutorManager.class, JobDefinitionRegistry.class, JobServerContext.class})
public final class TaskExecutorTest {
  private TaskExecutorManager mTaskExecutorManager;
  private JobDefinitionRegistry mRegistry;

  @Before
  public void before() {
    mTaskExecutorManager = PowerMockito.mock(TaskExecutorManager.class);
    mRegistry = PowerMockito.mock(JobDefinitionRegistry.class);
    Whitebox.setInternalState(JobDefinitionRegistry.class, "INSTANCE", mRegistry);
  }

  @Test
  public void runCompletion() throws Exception {
    long jobId = 1;
    int taskId = 2;
    JobConfig jobConfig = Mockito.mock(JobConfig.class);
    Serializable taskArgs = Lists.newArrayList(1);
    RunTaskContext context = Mockito.mock(RunTaskContext.class);
    Integer taskResult = 1;
    @SuppressWarnings("unchecked")
    JobDefinition<JobConfig, Serializable, Serializable> jobDefinition =
        Mockito.mock(JobDefinition.class);
    Mockito.when(mRegistry.getJobDefinition(jobConfig)).thenReturn(jobDefinition);
    Mockito.when(jobDefinition.runTask(Mockito.eq(jobConfig), Mockito.eq(taskArgs),
        Mockito.any(RunTaskContext.class))).thenReturn(taskResult);

    TaskExecutor executor =
        new TaskExecutor(jobId, taskId, jobConfig, taskArgs, context, mTaskExecutorManager);
    executor.run();

    Mockito.verify(jobDefinition).runTask(jobConfig, taskArgs, context);
    Mockito.verify(mTaskExecutorManager).notifyTaskCompletion(jobId, taskId, taskResult);
  }

  @Test
  public void runFailure() throws Exception {
    long jobId = 1;
    int taskId = 2;
    JobConfig jobConfig = Mockito.mock(JobConfig.class);
    Serializable taskArgs = Lists.newArrayList(1);
    RunTaskContext context = Mockito.mock(RunTaskContext.class);
    @SuppressWarnings("unchecked")
    JobDefinition<JobConfig, Serializable, Serializable> jobDefinition =
        Mockito.mock(JobDefinition.class);
    Mockito.when(mRegistry.getJobDefinition(jobConfig)).thenReturn(jobDefinition);
    Mockito.doThrow(new UnsupportedOperationException("failure")).when(jobDefinition)
        .runTask(jobConfig, taskArgs, context);

    TaskExecutor executor =
        new TaskExecutor(jobId, taskId, jobConfig, taskArgs, context, mTaskExecutorManager);
    executor.run();

    Mockito.verify(mTaskExecutorManager).notifyTaskFailure(Mockito.eq(jobId), Mockito.eq(taskId),
        Mockito.anyString());
  }

  @Test
  public void runCancelation() throws Exception {
    long jobId = 1;
    int taskId = 2;
    JobConfig jobConfig = Mockito.mock(JobConfig.class);
    Serializable taskArgs = Lists.newArrayList(1);
    RunTaskContext context = Mockito.mock(RunTaskContext.class);
    @SuppressWarnings("unchecked")
    JobDefinition<JobConfig, Serializable, Serializable> jobDefinition =
        Mockito.mock(JobDefinition.class);
    Mockito.when(mRegistry.getJobDefinition(jobConfig)).thenReturn(jobDefinition);
    Mockito.doThrow(new InterruptedException("interupt")).when(jobDefinition).runTask(jobConfig,
        taskArgs, context);

    TaskExecutor executor =
        new TaskExecutor(jobId, taskId, jobConfig, taskArgs, context, mTaskExecutorManager);
    executor.run();

    Mockito.verify(mTaskExecutorManager).notifyTaskCancellation(jobId, taskId);
  }
}
