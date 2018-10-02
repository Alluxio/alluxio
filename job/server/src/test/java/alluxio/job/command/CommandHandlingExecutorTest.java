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

package alluxio.job.command;

import alluxio.job.JobConfig;
import alluxio.job.JobWorkerContext;
import alluxio.job.TestJobConfig;
import alluxio.job.util.SerializationUtils;
import alluxio.thrift.JobCommand;
import alluxio.thrift.RunTaskCommand;
import alluxio.thrift.TaskInfo;
import alluxio.underfs.UfsManager;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.job.JobMasterClient;
import alluxio.worker.job.command.CommandHandlingExecutor;
import alluxio.worker.job.task.TaskExecutorManager;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Tests {@link CommandHandlingExecutor}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({TaskExecutorManager.class, WorkerNetAddress.class})
public final class CommandHandlingExecutorTest {
  private CommandHandlingExecutor mCommandHandlingExecutor;
  private JobMasterClient mJobMasterClient;
  private long mWorkerId;
  private TaskExecutorManager mTaskExecutorManager;
  private UfsManager mUfsManager;

  @Before
  public void before() {
    mWorkerId = 0;
    mJobMasterClient = Mockito.mock(JobMasterClient.class);
    mTaskExecutorManager = PowerMockito.mock(TaskExecutorManager.class);
    WorkerNetAddress workerNetAddress = PowerMockito.mock(WorkerNetAddress.class);
    mUfsManager = Mockito.mock(UfsManager.class);
    mCommandHandlingExecutor =
        new CommandHandlingExecutor(mTaskExecutorManager, mUfsManager, mJobMasterClient,
            workerNetAddress);
  }

  @Test
  public void heartbeat() throws Exception {
    JobCommand command = new JobCommand();
    RunTaskCommand runTaskCommand = new RunTaskCommand();
    long jobId = 1;
    runTaskCommand.setJobId(jobId);
    int taskId = 2;
    runTaskCommand.setTaskId(taskId);
    JobConfig jobConfig = new TestJobConfig("/test");
    runTaskCommand.setJobConfig(SerializationUtils.serialize(jobConfig));
    Serializable taskArgs = Lists.newArrayList(1);
    runTaskCommand.setTaskArgs(SerializationUtils.serialize(taskArgs));

    command.setRunTaskCommand(runTaskCommand);
    Mockito.when(mJobMasterClient.heartbeat(mWorkerId, Lists.<TaskInfo>newArrayList()))
        .thenReturn(Lists.newArrayList(command));

    mCommandHandlingExecutor.heartbeat();
    ExecutorService executorService =
        Whitebox.getInternalState(mCommandHandlingExecutor, "mCommandHandlingService");
    executorService.shutdown();
    Assert.assertTrue(executorService.awaitTermination(5000, TimeUnit.MILLISECONDS));

    Mockito.verify(mTaskExecutorManager).getAndClearTaskUpdates();
    Mockito.verify(mTaskExecutorManager).executeTask(Mockito.eq(jobId), Mockito.eq(taskId),
        Mockito.eq(jobConfig), Mockito.eq(taskArgs), Mockito.any(JobWorkerContext.class));
  }
}
