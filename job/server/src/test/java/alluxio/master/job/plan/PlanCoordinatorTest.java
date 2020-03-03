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

import static org.mockito.Mockito.mock;

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.collections.Pair;
import alluxio.grpc.JobCommand;
import alluxio.job.JobConfig;
import alluxio.job.plan.PlanDefinition;
import alluxio.job.plan.PlanDefinitionRegistry;
import alluxio.job.JobServerContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.master.job.command.CommandManager;
import alluxio.underfs.UfsManager;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
import java.util.Set;

/**
 * Tests {@link PlanCoordinator}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({PlanDefinitionRegistry.class, FileSystemContext.class})
public final class PlanCoordinatorTest {
  private WorkerInfo mWorkerInfo;
  private long mJobId;
  private JobConfig mJobconfig;
  private JobServerContext mJobServerContext;
  private CommandManager mCommandManager;
  private PlanDefinition<JobConfig, Serializable, Serializable> mPlanDefinition;
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
    PlanDefinition<JobConfig, Serializable, Serializable> mockPlanDefinition =
        Mockito.mock(PlanDefinition.class);
    PlanDefinitionRegistry singleton = PowerMockito.mock(PlanDefinitionRegistry.class);
    Whitebox.setInternalState(PlanDefinitionRegistry.class, "INSTANCE", singleton);
    Mockito.when(singleton.getJobDefinition(mJobconfig)).thenReturn(mockPlanDefinition);
    mPlanDefinition = mockPlanDefinition;

    // Create test worker.
    mWorkerInfo = new WorkerInfo();
    mWorkerInfo.setId(0);
    mWorkerInfoList = Lists.newArrayList(mWorkerInfo);
  }

  @Test
  public void createJobCoordinator() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    PlanCoordinator.create(
        mCommandManager, mJobServerContext, mWorkerInfoList, mJobId, mJobconfig, null);

    List<JobCommand> commands = mCommandManager.pollAllPendingCommands(mWorkerInfo.getId());
    Assert.assertEquals(1, commands.size());
    Assert.assertEquals(mJobId, commands.get(0).getRunTaskCommand().getJobId());
    Assert.assertEquals(0, commands.get(0).getRunTaskCommand().getTaskId());
  }

  @Test
  public void updateStatusFailure() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    PlanCoordinator planCoordinator = PlanCoordinator.create(mCommandManager, mJobServerContext,
        mWorkerInfoList, mJobId, mJobconfig, null);
    setTasksWithStatuses(planCoordinator, Status.RUNNING, Status.FAILED, Status.COMPLETED);

    Assert.assertEquals(Status.FAILED, planCoordinator.getPlanInfoWire(true).getStatus());
    Assert.assertTrue(
        planCoordinator.getPlanInfoWire(true).getErrorMessage().contains("Task execution failed"));
  }

  @Test
  public void updateStatusFailureOverCancel() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    PlanCoordinator planCoordinator = PlanCoordinator.create(mCommandManager, mJobServerContext,
        mWorkerInfoList, mJobId, mJobconfig, null);
    setTasksWithStatuses(planCoordinator, Status.RUNNING, Status.FAILED, Status.COMPLETED);

    Assert.assertEquals(Status.FAILED, planCoordinator.getPlanInfoWire(true).getStatus());
  }

  @Test
  public void updateStatusCancel() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    PlanCoordinator planCoordinator = PlanCoordinator.create(mCommandManager, mJobServerContext,
        mWorkerInfoList, mJobId, mJobconfig, null);
    setTasksWithStatuses(planCoordinator, Status.CANCELED, Status.RUNNING, Status.COMPLETED);

    Assert.assertEquals(Status.CANCELED, planCoordinator.getPlanInfoWire(true).getStatus());
  }

  @Test
  public void updateStatusRunning() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    PlanCoordinator planCoordinator = PlanCoordinator.create(mCommandManager, mJobServerContext,
        mWorkerInfoList, mJobId, mJobconfig, null);
    setTasksWithStatuses(planCoordinator, Status.COMPLETED, Status.RUNNING, Status.COMPLETED);

    Assert.assertEquals(Status.RUNNING, planCoordinator.getPlanInfoWire(true).getStatus());
  }

  @Test
  public void updateStatusCompleted() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    PlanCoordinator planCoordinator = PlanCoordinator.create(mCommandManager, mJobServerContext,
        mWorkerInfoList, mJobId, mJobconfig, null);
    setTasksWithStatuses(planCoordinator, Status.COMPLETED, Status.COMPLETED, Status.COMPLETED);

    Assert.assertEquals(Status.COMPLETED, planCoordinator.getPlanInfoWire(true).getStatus());
  }

  @Test
  public void updateStatusJoinFailure() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    Mockito
        .when(mPlanDefinition.join(Mockito.eq(mJobconfig),
            Mockito.anyMapOf(WorkerInfo.class, Serializable.class)))
        .thenThrow(new UnsupportedOperationException("test exception"));
    PlanCoordinator planCoordinator = PlanCoordinator.create(mCommandManager, mJobServerContext,
        mWorkerInfoList, mJobId, mJobconfig, null);
    setTasksWithStatuses(planCoordinator, Status.COMPLETED, Status.COMPLETED, Status.COMPLETED);

    Assert.assertEquals(Status.FAILED, planCoordinator.getPlanInfoWire(true).getStatus());
    Assert.assertEquals("test exception", planCoordinator.getPlanInfoWire(true).getErrorMessage());
  }

  @Test
  public void noTasks() throws Exception {
    mockSelectExecutors();
    PlanCoordinator planCoordinator = PlanCoordinator.create(mCommandManager, mJobServerContext,
        mWorkerInfoList, mJobId, mJobconfig, null);
    Assert.assertEquals(Status.COMPLETED, planCoordinator.getPlanInfoWire(true).getStatus());
  }

  @Test
  public void failWorker() throws Exception {
    mockSelectExecutors(mWorkerInfo);
    PlanCoordinator planCoordinator = PlanCoordinator.create(mCommandManager, mJobServerContext,
        mWorkerInfoList, mJobId, mJobconfig, null);
    planCoordinator.failTasksForWorker(mWorkerInfo.getId());
    Assert.assertEquals(Status.FAILED, planCoordinator.getPlanInfoWire(true).getStatus());
  }

  /**
   * @param workerInfos the worker infos to return from the mocked selectExecutors method
   */
  private void mockSelectExecutors(WorkerInfo... workerInfos) throws Exception {
    Set<Pair<WorkerInfo, Serializable>> taskAddressToArgs = Sets.newHashSet();
    for (WorkerInfo workerInfo : workerInfos) {
      taskAddressToArgs.add(new Pair<>(workerInfo, null));
    }
    Mockito
        .when(mPlanDefinition.selectExecutors(Mockito.eq(mJobconfig),
            Mockito.eq(Lists.newArrayList(mWorkerInfo)), Mockito.any(SelectExecutorsContext.class)))
        .thenReturn(taskAddressToArgs);
  }

  private void setTasksWithStatuses(PlanCoordinator planCoordinator, Status... statuses)
      throws Exception {
    List<TaskInfo> taskInfos = new ArrayList<>();
    int taskId = 0;
    for (Status status : statuses) {
      taskInfos.add(new TaskInfo().setTaskId(taskId++).setJobId(mJobId).setStatus(status));
    }
    planCoordinator.updateTasks(taskInfos);
  }
}
