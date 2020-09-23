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

package alluxio.master.job.workflow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.exception.status.ResourceExhaustedException;
import alluxio.job.JobConfig;
import alluxio.job.JobServerContext;
import alluxio.job.SleepJobConfig;
import alluxio.job.TestPlanConfig;
import alluxio.job.plan.meta.PlanInfo;
import alluxio.job.plan.PlanConfig;
import alluxio.job.wire.Status;
import alluxio.job.wire.WorkflowInfo;
import alluxio.job.workflow.composite.CompositeConfig;
import alluxio.master.job.JobMaster;
import alluxio.master.job.command.CommandManager;
import alluxio.master.job.plan.PlanTracker;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class WorkflowTrackerTest {

  private static final long CAPACITY = 5;
  private static final long RETENTION_TIME = 0;
  private static final long PURGE_CONUT = -1;

  private PlanTracker mPlanTracker;
  private WorkflowTracker mWorkflowTracker;
  private JobMaster mMockJobMaster;
  private long mJobIdCounter;

  private ArrayList<WorkerInfo> mWorkers;
  private CommandManager mCommandManager;
  private JobServerContext mMockJobServerContext;

  @Before
  public void before() throws Exception {
    mMockJobMaster = mock(JobMaster.class);
    mWorkflowTracker = new WorkflowTracker(mMockJobMaster);
    mPlanTracker = new PlanTracker(CAPACITY, RETENTION_TIME, PURGE_CONUT, mWorkflowTracker);

    mJobIdCounter = 100;
    when(mMockJobMaster.getNewJobId()).thenAnswer(invocation -> mJobIdCounter++);

    mWorkers = Lists.newArrayList(new WorkerInfo());
    mCommandManager = new CommandManager();
    mMockJobServerContext = mock(JobServerContext.class);
  }

  @Test
  public void testEmpty() throws Exception {
    ArrayList<JobConfig> jobs = Lists.newArrayList();
    CompositeConfig config = new CompositeConfig(jobs, true);
    mWorkflowTracker.run(config, 0);
    WorkflowInfo info = mWorkflowTracker.getStatus(0, true);

    assertEquals(Status.COMPLETED, info.getStatus());
  }

  @Test
  public void testBasic() throws Exception {

    ArrayList<JobConfig> jobs = Lists.newArrayList();

    TestPlanConfig child1 = new TestPlanConfig("1");
    TestPlanConfig child2 = new TestPlanConfig("2");
    jobs.add(child1);
    jobs.add(child2);

    CompositeConfig config = new CompositeConfig(jobs, true);

    mWorkflowTracker.run(config, 0);

    verify(mMockJobMaster).run(child1, 100);

    WorkflowInfo info = mWorkflowTracker.getStatus(0, true);

    assertEquals(Status.RUNNING, info.getStatus());

    verify(mMockJobMaster, never()).run(child2, 101);

    PlanInfo plan100 = new PlanInfo(100, child1, null);
    plan100.setStatus(Status.COMPLETED);
    mWorkflowTracker.onPlanStatusChange(plan100);

    verify(mMockJobMaster).run(child2, 101);

    assertEquals(Status.RUNNING, mWorkflowTracker.getStatus(0, true).getStatus());

    PlanInfo plan101 = new PlanInfo(101, child2, null);
    plan101.setStatus(Status.COMPLETED);
    mWorkflowTracker.onPlanStatusChange(plan101);

    assertEquals(Status.COMPLETED, mWorkflowTracker.getStatus(0, true).getStatus());
  }

  @Test
  public void testCleanup() throws Exception {
    SleepJobConfig jobConfig = new SleepJobConfig(1);
    mPlanTracker.run(jobConfig, mCommandManager, mMockJobServerContext, mWorkers, 1);

    jobConfig = new SleepJobConfig(1);
    mPlanTracker.run(jobConfig, mCommandManager, mMockJobServerContext, mWorkers, 2);

    jobConfig = new SleepJobConfig(1);
    mPlanTracker.run(jobConfig, mCommandManager, mMockJobServerContext, mWorkers, 3);

    doAnswer(invocation -> {
      PlanConfig config = invocation.getArgument(0, PlanConfig.class);
      long jobId = invocation.getArgument(1, Long.class);

      mPlanTracker.run(config, mCommandManager, mMockJobServerContext, mWorkers, jobId);
      return null;
    }).when(mMockJobMaster).run(any(PlanConfig.class), any(Long.class));

    ArrayList<JobConfig> jobs = Lists.newArrayList();

    SleepJobConfig child1 = new SleepJobConfig(1);
    SleepJobConfig child2 = new SleepJobConfig(2);
    jobs.add(child1);
    jobs.add(child2);

    CompositeConfig config = new CompositeConfig(jobs, false);

    mWorkflowTracker.run(config, 0);

    try {
      mPlanTracker.run(new SleepJobConfig(1), mCommandManager, mMockJobServerContext, mWorkers, 4);
      fail();
    } catch (ResourceExhaustedException e) {
      // Should fail
    }

    mPlanTracker.coordinators().stream().filter(coordinator -> coordinator.getJobId() == 100)
        .findFirst().get().setJobAsFailed("TestError", "failed");

    mPlanTracker.run(new SleepJobConfig(1), mCommandManager, mMockJobServerContext, mWorkers, 4);

    assertNotNull(mWorkflowTracker.getStatus(0, true));

    try {
      mPlanTracker.run(new SleepJobConfig(1), mCommandManager, mMockJobServerContext, mWorkers, 5);
      fail();
    } catch (ResourceExhaustedException e) {
      // Should fail
    }

    mPlanTracker.coordinators().stream().filter(coordinator -> coordinator.getJobId() == 101)
        .findFirst().get().setJobAsFailed("TestError", "failed");

    mPlanTracker.run(new SleepJobConfig(1), mCommandManager, mMockJobServerContext, mWorkers, 5);

    assertNull(mWorkflowTracker.getStatus(100, true));
  }
}
