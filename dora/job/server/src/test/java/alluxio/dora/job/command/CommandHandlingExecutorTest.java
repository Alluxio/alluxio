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

package alluxio.dora.job.command;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;

import alluxio.dora.AlluxioMockUtil;
import alluxio.dora.client.file.FileSystem;
import alluxio.dora.client.file.FileSystemContext;
import alluxio.dora.grpc.JobCommand;
import alluxio.dora.grpc.RunTaskCommand;
import alluxio.dora.job.JobConfig;
import alluxio.dora.job.JobServerContext;
import alluxio.dora.job.RunTaskContext;
import alluxio.dora.job.TestPlanConfig;
import alluxio.dora.job.util.SerializationUtils;
import alluxio.dora.job.wire.JobWorkerHealth;
import alluxio.dora.underfs.UfsManager;
import alluxio.dora.wire.WorkerNetAddress;
import alluxio.dora.worker.job.JobMasterClient;
import alluxio.dora.worker.job.command.CommandHandlingExecutor;
import alluxio.dora.worker.job.task.TaskExecutorManager;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Tests {@link CommandHandlingExecutor}.
 */
public final class CommandHandlingExecutorTest {
  private CommandHandlingExecutor mCommandHandlingExecutor;
  private JobMasterClient mJobMasterClient;
  private TaskExecutorManager mTaskExecutorManager;
  private UfsManager mUfsManager;
  private FileSystemContext mFileSystemContext;
  private FileSystem mFileSystem;

  @Before
  public void before() {

    mJobMasterClient = mock(JobMasterClient.class);
    mTaskExecutorManager = mock(TaskExecutorManager.class);
    WorkerNetAddress workerNetAddress = mock(WorkerNetAddress.class);
    mUfsManager = mock(UfsManager.class);
    mFileSystemContext = mock(FileSystemContext.class);
    mFileSystem = mock(FileSystem.class);
    JobServerContext ctx = new JobServerContext(mFileSystem, mFileSystemContext, mUfsManager);
    mCommandHandlingExecutor =
        new CommandHandlingExecutor(ctx, mTaskExecutorManager, mJobMasterClient, workerNetAddress);
  }

  @Test
  public void heartbeat() throws Exception {
    JobCommand.Builder command = JobCommand.newBuilder();
    RunTaskCommand.Builder runTaskCommand = RunTaskCommand.newBuilder();
    long jobId = 1;
    runTaskCommand.setJobId(jobId);
    long taskId = 2;
    runTaskCommand.setTaskId(taskId);
    JobConfig jobConfig = new TestPlanConfig("/test");
    runTaskCommand.setJobConfig(ByteString.copyFrom(SerializationUtils.serialize(jobConfig)));
    Serializable taskArgs = Lists.newArrayList(1);
    runTaskCommand.setTaskArgs(ByteString.copyFrom(SerializationUtils.serialize(taskArgs)));

    command.setRunTaskCommand(runTaskCommand);

    Mockito.when(mJobMasterClient.heartbeat(any(JobWorkerHealth.class), eq(Lists.newArrayList())))
        .thenReturn(Lists.newArrayList(command.build()));

    mCommandHandlingExecutor.heartbeat();
    ExecutorService executorService = AlluxioMockUtil.getInternalState(
        mCommandHandlingExecutor, "mCommandHandlingService");
    executorService.shutdown();
    Assert.assertTrue(executorService.awaitTermination(5000, TimeUnit.MILLISECONDS));

    Mockito.verify(mTaskExecutorManager).getAndClearTaskUpdates();
    Mockito.verify(mTaskExecutorManager).executeTask(Mockito.eq(jobId), Mockito.eq(taskId),
        Mockito.eq(runTaskCommand.build()), Mockito.any(RunTaskContext.class));
  }
}
