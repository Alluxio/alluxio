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

package alluxio.dora.worker.job.task;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.dora.AlluxioMockUtil;
import alluxio.dora.grpc.RunTaskCommand;
import alluxio.dora.job.JobConfig;
import alluxio.dora.job.RunTaskContext;
import alluxio.dora.job.SleepJobConfig;
import alluxio.dora.job.plan.PlanDefinition;
import alluxio.dora.job.plan.PlanDefinitionRegistry;
import alluxio.dora.job.util.SerializationUtils;

import alluxio.dora.worker.job.task.TaskExecutor;
import alluxio.dora.worker.job.task.TaskExecutorManager;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;

/**
 * Tests {@link TaskExecutor}.
 */
public final class TaskExecutorTest {
  private TaskExecutorManager mTaskExecutorManager;
  private PlanDefinitionRegistry mRegistry;

  @Before
  public void before() {
    mTaskExecutorManager = mock(TaskExecutorManager.class);
    mRegistry = mock(PlanDefinitionRegistry.class);
    AlluxioMockUtil.setInternalState(PlanDefinitionRegistry.class, "INSTANCE", mRegistry);
  }

  @Test
  public void runCompletion() throws Exception {
    long jobId = 1;
    long taskId = 2;
    JobConfig jobConfig = mock(JobConfig.class);

    Serializable taskArgs = Lists.newArrayList(1);
    RunTaskContext context = mock(RunTaskContext.class);
    Integer taskResult = 1;
    @SuppressWarnings("unchecked")
    PlanDefinition<JobConfig, Serializable, Serializable> planDefinition =
        mock(PlanDefinition.class);
    when(mRegistry.getJobDefinition(any(JobConfig.class))).thenReturn(planDefinition);
    when(planDefinition.runTask(any(JobConfig.class), eq(taskArgs), any(RunTaskContext.class)))
        .thenReturn(taskResult);

    RunTaskCommand command = RunTaskCommand.newBuilder()
        .setJobConfig(ByteString.copyFrom(SerializationUtils.serialize(jobConfig)))
        .setTaskArgs(ByteString.copyFrom(SerializationUtils.serialize(taskArgs))).build();

    TaskExecutor executor =
        new TaskExecutor(jobId, taskId, command, context, mTaskExecutorManager);
    executor.run();

    verify(planDefinition).runTask(any(JobConfig.class), eq(taskArgs), eq(context));
    verify(mTaskExecutorManager).notifyTaskCompletion(jobId, taskId, taskResult);
  }

  @Test
  public void runFailure() throws Exception {
    long jobId = 1;
    long taskId = 2;
    JobConfig jobConfig = new SleepJobConfig(10);
    Serializable taskArgs = Lists.newArrayList(1);
    RunTaskContext context = mock(RunTaskContext.class);
    @SuppressWarnings("unchecked")
    PlanDefinition<JobConfig, Serializable, Serializable> planDefinition =
        mock(PlanDefinition.class);
    when(mRegistry.getJobDefinition(eq(jobConfig))).thenReturn(planDefinition);
    when(planDefinition.runTask(eq(jobConfig), any(Serializable.class), any(RunTaskContext.class)))
        .thenThrow(new UnsupportedOperationException("failure"));

    RunTaskCommand command = RunTaskCommand.newBuilder()
        .setJobConfig(ByteString.copyFrom(SerializationUtils.serialize(jobConfig)))
        .setTaskArgs(ByteString.copyFrom(SerializationUtils.serialize(taskArgs))).build();

    TaskExecutor executor =
        new TaskExecutor(jobId, taskId, command, context, mTaskExecutorManager);
    executor.run();

    verify(mTaskExecutorManager).notifyTaskFailure(eq(jobId), eq(taskId), any());
  }
}
