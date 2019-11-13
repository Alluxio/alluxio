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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.job.JobConfig;
import alluxio.job.TestPlanConfig;
import alluxio.job.wire.PlanInfo;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.job.wire.WorkflowInfo;
import alluxio.job.workflow.composite.CompositeConfig;
import alluxio.master.job.JobMaster;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class WorkflowTrackerTest {

  private WorkflowTracker mTracker;
  private JobMaster mMockJobMaster;
  private long mJobIdCounter;

  @Before
  public void before() {
    mMockJobMaster = mock(JobMaster.class);
    mTracker = new WorkflowTracker(mMockJobMaster);

    mJobIdCounter = 100;

    when(mMockJobMaster.getNewJobId()).thenAnswer(invocation -> mJobIdCounter++);
  }

  @Test
  public void testBasic() throws Exception {

    ArrayList<JobConfig> jobs = Lists.newArrayList();

    TestPlanConfig child1 = new TestPlanConfig("1");
    TestPlanConfig child2 = new TestPlanConfig("2");
    jobs.add(child1);
    jobs.add(child2);

    CompositeConfig config = new CompositeConfig(jobs, true);

    mTracker.run(config, 0);

    verify(mMockJobMaster).run(child1, 100);

    WorkflowInfo info = mTracker.getStatus(0);

    assertEquals(Status.RUNNING, info.getStatus());

    verify(mMockJobMaster, never()).run(child2, 101);

    TaskInfo task100 = new TaskInfo(100, 0, Status.COMPLETED);
    ArrayList<TaskInfo> taskInfos = Lists.newArrayList(task100);

    PlanInfo plan100 = new PlanInfo(100, "test", Status.COMPLETED, 0, null);
    when(mMockJobMaster.getStatus(100)).thenReturn(plan100);
    mTracker.workerHeartbeat(taskInfos);

    verify(mMockJobMaster).run(child2, 101);

    assertEquals(Status.RUNNING, mTracker.getStatus(0).getStatus());

    TaskInfo task101 = new TaskInfo(101, 0, Status.COMPLETED);
    taskInfos = Lists.newArrayList(task101);
    PlanInfo plan101 = new PlanInfo(101, "test", Status.COMPLETED, 0, null);
    when(mMockJobMaster.getStatus(101)).thenReturn(plan101);
    mTracker.workerHeartbeat(taskInfos);

    assertEquals(Status.COMPLETED, mTracker.getStatus(0).getStatus());
  }
}
