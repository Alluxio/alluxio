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

import static org.mockito.Mockito.mock;

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.grpc.JobCommand;
import alluxio.job.JobConfig;
import alluxio.job.JobDefinition;
import alluxio.job.JobDefinitionRegistry;
import alluxio.job.JobServerContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.master.job.command.CommandManager;
import alluxio.underfs.UfsManager;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tests {@link JobCoordinator}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({JobDefinitionRegistry.class, FileSystemContext.class})
public final class JobCoordinatorTest {
  private WorkerInfo mWorkerInfo;
  private long mJobId;
  private JobConfig mJobconfig;
  private JobServerContext mJobServerContext;
  private CommandManager mCommandManager;
  private JobDefinition<JobConfig, Serializable, Serializable> mJobDefinition;
  private List<WorkerInfo> mWorkerInfoList;

  @Before
  public void before() throws Exception {
    mCommandManager = new CommandManager();

    // Create mock JobServerContext
    FileSystem fs = mock(FileSystem.class);
    FileSystemContext fsCtx = PowerMockito.mock(FileSystemContext.class);
    UfsManager ufsManager = Mockito.mock(UfsManager.class);
    mJobServerContext = new JobServerContext(fs, fsCtx, ufsManager);

    // Create mock job info.
    mJobconfig = Mockito.mock(JobConfig.class, Mockito.withSettings().serializable());
    Mockito.when(mJobconfig.getName()).thenReturn("mock");
    mJobId = 1;

    // Create mock job definition.
    @SuppressWarnings("unchecked")
    JobDefinition<JobConfig, Serializable, Serializable> mockJobDefinition =
        Mockito.mock(JobDefinition.class);
    JobDefinitionRegistry singleton = PowerMockito.mock(JobDefinitionRegistry.class);
    Whitebox.setInternalState(JobDefinitionRegistry.class, "INSTANCE", singleton);
    Mockito.when(singleton.getJobDefinition(mJobconfig)).thenReturn(mockJobDefinition);
    mJobDefinition = mockJobDefinition;

    // Create test worker.
    mWorkerInfo = new WorkerInfo();
    mWorkerInfo.setId(0);
    mWorkerInfoList = Lists.newArrayList(mWorkerInfo);
  }

  @Test
  public void createJobCoordinator() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator.create(
        mCommandManager, mJobServerContext, mWorkerInfoList, mJobId, mJobconfig, null);

    List<JobCommand> commands = mCommandManager.pollAllPendingCommands(mWorkerInfo.getId());
    Assert.assertEquals(1, commands.size());
    Assert.assertEquals(mJobId, commands.get(0).getRunTaskCommand().getJobId());
    Assert.assertEquals(0, commands.get(0).getRunTaskCommand().getTaskId());
  }

  @Test
  public void updateStatusFailure() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator jobCoordinator = JobCoordinator.create(mCommandManager, mJobServerContext,
        mWorkerInfoList, mJobId, mJobconfig, null);
    setTasksWithStatuses(jobCoordinator, Status.RUNNING, Status.FAILED, Status.COMPLETED);

    Assert.assertEquals(Status.FAILED, jobCoordinator.getJobInfoWire().getStatus());
    Assert.assertTrue(
        jobCoordinator.getJobInfoWire().getErrorMessage().contains("Task execution failed"));
  }

  @Test
  public void updateStatusFailureOverCancel() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator jobCoordinator = JobCoordinator.create(mCommandManager, mJobServerContext,
        mWorkerInfoList, mJobId, mJobconfig, null);
    setTasksWithStatuses(jobCoordinator, Status.RUNNING, Status.FAILED, Status.COMPLETED);

    Assert.assertEquals(Status.FAILED, jobCoordinator.getJobInfoWire().getStatus());
  }

  @Test
  public void updateStatusCancel() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator jobCoordinator = JobCoordinator.create(mCommandManager, mJobServerContext,
        mWorkerInfoList, mJobId, mJobconfig, null);
    setTasksWithStatuses(jobCoordinator, Status.CANCELED, Status.RUNNING, Status.COMPLETED);

    Assert.assertEquals(Status.CANCELED, jobCoordinator.getJobInfoWire().getStatus());
  }

  @Test
  public void updateStatusRunning() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator jobCoordinator = JobCoordinator.create(mCommandManager, mJobServerContext,
        mWorkerInfoList, mJobId, mJobconfig, null);
    setTasksWithStatuses(jobCoordinator, Status.COMPLETED, Status.RUNNING, Status.COMPLETED);

    Assert.assertEquals(Status.RUNNING, jobCoordinator.getJobInfoWire().getStatus());
  }

  @Test
  public void updateStatusCompleted() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator jobCoordinator = JobCoordinator.create(mCommandManager, mJobServerContext,
        mWorkerInfoList, mJobId, mJobconfig, null);
    setTasksWithStatuses(jobCoordinator, Status.COMPLETED, Status.COMPLETED, Status.COMPLETED);

    Assert.assertEquals(Status.COMPLETED, jobCoordinator.getJobInfoWire().getStatus());
    Mockito.verify(mJobDefinition).join(Mockito.eq(jobCoordinator.getJobInfoWire().getJobConfig()),
        Mockito.anyMapOf(WorkerInfo.class, Serializable.class));
  }

  @Test
  public void updateStatusJoinFailure() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    Mockito
        .when(mJobDefinition.join(Mockito.eq(mJobconfig),
            Mockito.anyMapOf(WorkerInfo.class, Serializable.class)))
        .thenThrow(new UnsupportedOperationException("test exception"));
    JobCoordinator jobCoordinator = JobCoordinator.create(mCommandManager, mJobServerContext,
        mWorkerInfoList, mJobId, mJobconfig, null);
    setTasksWithStatuses(jobCoordinator, Status.COMPLETED, Status.COMPLETED, Status.COMPLETED);

    Assert.assertEquals(Status.FAILED, jobCoordinator.getJobInfoWire().getStatus());
    Assert.assertEquals("test exception", jobCoordinator.getJobInfoWire().getErrorMessage());
  }

  @Test
  public void noTasks() throws Exception {
    mockSelectExecutors();
    JobCoordinator jobCoordinator = JobCoordinator.create(mCommandManager, mJobServerContext,
        mWorkerInfoList, mJobId, mJobconfig, null);
    Assert.assertEquals(Status.COMPLETED, jobCoordinator.getJobInfoWire().getStatus());
  }

  @Test
  public void failWorker() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator jobCoordinator = JobCoordinator.create(mCommandManager, mJobServerContext,
        mWorkerInfoList, mJobId, mJobconfig, null);
    jobCoordinator.failTasksForWorker(mWorkerInfo.getId());
    Assert.assertEquals(Status.FAILED, jobCoordinator.getJobInfoWire().getStatus());
  }

  /**
   * @param workerInfos the worker infos to return from the mocked selectExecutors method
   */
  private void mockSelectExecutors(WorkerInfo... workerInfos) throws Exception {
    Map<WorkerInfo, Serializable> taskAddressToArgs = Maps.newHashMap();
    for (WorkerInfo workerInfo : workerInfos) {
      taskAddressToArgs.put(workerInfo, null);
    }
    Mockito
        .when(mJobDefinition.selectExecutors(Mockito.eq(mJobconfig),
            Mockito.eq(Lists.newArrayList(mWorkerInfo)), Mockito.any(SelectExecutorsContext.class)))
        .thenReturn(taskAddressToArgs);
  }

  private void setTasksWithStatuses(JobCoordinator jobCoordinator, Status... statuses)
      throws Exception {
    List<TaskInfo> taskInfos = new ArrayList<>();
    int taskId = 0;
    for (Status status : statuses) {
      taskInfos.add(new TaskInfo().setTaskId(taskId++).setJobId(mJobId).setStatus(status));
    }
    jobCoordinator.updateTasks(taskInfos);
  }
}
