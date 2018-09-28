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
import alluxio.job.JobMasterContext;
import alluxio.job.meta.JobInfo;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.master.job.command.CommandManager;
import alluxio.thrift.JobCommand;
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
import java.util.List;
import java.util.Map;

/**
 * Tests {@link JobCoordinator}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({JobDefinitionRegistry.class})
public final class JobCoordinatorTest {
  private WorkerInfo mWorkerInfo;
  private long mJobId;
  private JobInfo mJobInfo;
  private CommandManager mCommandManager;
  private List<WorkerInfo> mWorkerInfoList;
  private JobDefinition<JobConfig, Serializable, Serializable> mJobDefinition;
  private UfsManager mUfsManager;

  @Before
  public void before() throws Exception {
    mCommandManager = new CommandManager();

    // Create mock job info.
    JobConfig jobConfig = Mockito.mock(JobConfig.class, Mockito.withSettings().serializable());
    Mockito.when(jobConfig.getName()).thenReturn("mock");
    mJobId = 1;
    mJobInfo = new JobInfo(mJobId, jobConfig, null);

    // Create mock job definition.
    @SuppressWarnings("unchecked")
    JobDefinition<JobConfig, Serializable, Serializable> mockJobDefinition =
        Mockito.mock(JobDefinition.class);
    JobDefinitionRegistry singleton = PowerMockito.mock(JobDefinitionRegistry.class);
    Whitebox.setInternalState(JobDefinitionRegistry.class, "INSTANCE", singleton);
    Mockito.when(singleton.getJobDefinition(jobConfig)).thenReturn(mockJobDefinition);
    mJobDefinition = mockJobDefinition;

    // Create test worker.
    mWorkerInfo = new WorkerInfo();
    mWorkerInfo.setId(0);
    mWorkerInfoList = Lists.newArrayList(mWorkerInfo);

    mUfsManager = Mockito.mock(UfsManager.class);
  }

  @Test
  public void createJobCoordinator() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator.create(mCommandManager, mUfsManager, mWorkerInfoList, mJobInfo);

    List<JobCommand> commands = mCommandManager.pollAllPendingCommands(mWorkerInfo.getId());
    Assert.assertEquals(1, commands.size());
    Assert.assertEquals(mJobId, commands.get(0).getRunTaskCommand().getJobId());
    Assert.assertEquals(0, commands.get(0).getRunTaskCommand().getTaskId());
  }

  @Test
  public void updateStatusFailure() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, mUfsManager, mWorkerInfoList, mJobInfo);
    setTasksWithStatuses(Status.RUNNING, Status.FAILED, Status.COMPLETED);
    jobCoordinator.updateStatus();

    Assert.assertEquals(Status.FAILED, mJobInfo.getStatus());
    Assert.assertTrue(mJobInfo.getErrorMessage().contains("Task execution failed"));
  }

  @Test
  public void updateStatusFailureOverCancel() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, mUfsManager, mWorkerInfoList, mJobInfo);
    setTasksWithStatuses(Status.RUNNING, Status.FAILED, Status.COMPLETED);
    jobCoordinator.updateStatus();

    Assert.assertEquals(Status.FAILED, mJobInfo.getStatus());
  }

  @Test
  public void updateStatusCancel() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, mUfsManager, mWorkerInfoList, mJobInfo);
    setTasksWithStatuses(Status.CANCELED, Status.RUNNING, Status.COMPLETED);
    jobCoordinator.updateStatus();

    Assert.assertEquals(Status.CANCELED, mJobInfo.getStatus());
  }

  @Test
  public void updateStatusRunning() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, mUfsManager, mWorkerInfoList, mJobInfo);
    setTasksWithStatuses(Status.COMPLETED, Status.RUNNING, Status.COMPLETED);
    jobCoordinator.updateStatus();

    Assert.assertEquals(Status.RUNNING, mJobInfo.getStatus());
  }

  @Test
  public void updateStatusCompleted() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, mUfsManager, mWorkerInfoList, mJobInfo);
    setTasksWithStatuses(Status.COMPLETED, Status.COMPLETED, Status.COMPLETED);
    jobCoordinator.updateStatus();

    Assert.assertEquals(Status.COMPLETED, mJobInfo.getStatus());
    Mockito.verify(mJobDefinition).join(Mockito.eq(mJobInfo.getJobConfig()),
        Mockito.anyMapOf(WorkerInfo.class, Serializable.class));
  }

  @Test
  public void updateStatusJoinFailure() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    Mockito
        .when(mJobDefinition.join(Mockito.eq(mJobInfo.getJobConfig()),
            Mockito.anyMapOf(WorkerInfo.class, Serializable.class)))
        .thenThrow(new UnsupportedOperationException("test exception"));
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, mUfsManager, mWorkerInfoList, mJobInfo);
    setTasksWithStatuses(Status.COMPLETED, Status.COMPLETED, Status.COMPLETED);
    jobCoordinator.updateStatus();

    Assert.assertEquals(Status.FAILED, mJobInfo.getStatus());
    Assert.assertEquals("test exception", mJobInfo.getErrorMessage());
  }

  @Test
  public void noTasks() throws Exception {
    mockSelectExecutors();
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, mUfsManager, mWorkerInfoList, mJobInfo);
    Assert.assertEquals(Status.COMPLETED, mJobInfo.getStatus());
  }

  @Test
  public void failWorker() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, mUfsManager, mWorkerInfoList, mJobInfo);
    jobCoordinator.failTasksForWorker(mWorkerInfo.getId());
    Assert.assertEquals(Status.FAILED, mJobInfo.getStatus());
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
        .when(mJobDefinition.selectExecutors(Mockito.eq(mJobInfo.getJobConfig()),
            Mockito.eq(Lists.newArrayList(mWorkerInfo)), Mockito.any(JobMasterContext.class)))
        .thenReturn(taskAddressToArgs);
  }

  private void setTasksWithStatuses(Status... statuses) throws Exception {
    int taskId = 0;
    for (Status status : statuses) {
      mJobInfo.setTaskInfo(taskId, new TaskInfo().setJobId(mJobId).setStatus(status));
      taskId++;
    }
  }
}
