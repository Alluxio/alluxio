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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import alluxio.exception.status.ResourceExhaustedException;
import alluxio.job.JobServerContext;
import alluxio.job.SleepJobConfig;
import alluxio.job.meta.JobIdGenerator;
import alluxio.job.wire.Status;
import alluxio.master.job.command.CommandManager;
import alluxio.master.job.workflow.WorkflowTracker;
import alluxio.util.FormatUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.powermock.reflect.Whitebox;

import java.util.List;
import java.util.Queue;

public class PlanTrackerTest {

  private static final long CAPACITY = 25;
  private static final long RETENTION_TIME = 0;
  private static final long PURGE_CONUT = -1;
  private List<WorkerInfo> mWorkers;
  private PlanTracker mTracker;
  private WorkflowTracker mMockWorkflowTracker;
  private CommandManager mCommandManager;
  private JobServerContext mMockJobServerContext;
  private JobIdGenerator mJobIdGenerator;

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Before
  public void before() {
    mMockWorkflowTracker = mock(WorkflowTracker.class);
    mTracker = new PlanTracker(CAPACITY, RETENTION_TIME, PURGE_CONUT, mMockWorkflowTracker);
    mCommandManager = new CommandManager();
    mMockJobServerContext = mock(JobServerContext.class);
    mWorkers = Lists.newArrayList(new WorkerInfo());
    mJobIdGenerator = new JobIdGenerator();
  }

  @Test
  public void testAddJobIncreasesCount() throws Exception {
    assertEquals("tracker should be empty", 0, mTracker.coordinators().size());
    addJob(100);
    assertEquals("tracker should have one job", 1, mTracker.coordinators().size());
  }

  @Test
  public void testAddJobUpToCapacity() throws Exception {
    assertEquals("tracker should be empty", 0, mTracker.coordinators().size());
    fillJobTracker(CAPACITY);
    mException.expect(ResourceExhaustedException.class);
    addJob(100);
  }

  @Test
  public void testJobListing() throws Exception {
    assertEquals("tracker should be empty", 0, mTracker.coordinators().size());
    assertEquals(0, mTracker.findJobs("Sleep", ImmutableList.of(Status.CREATED)).size());
    addJob(500);
    assertEquals(1, mTracker.findJobs("Sleep", ImmutableList.of(Status.CREATED)).size());
    finishAllJobs();
    assertEquals(0, mTracker.findJobs("Sleep", ImmutableList.of(Status.CREATED)).size());
    assertEquals(1, mTracker.findJobs("Sleep", ImmutableList.of(Status.FAILED)).size());
  }

  @Test
  public void testAddAndPurge() throws Exception {
    assertEquals("tracker should be empty", 0, mTracker.coordinators().size());
    fillJobTracker(CAPACITY);
    try {
      addJob(100);
      fail("Should have failed to add a job over capacity");
    } catch (ResourceExhaustedException e) {
      // Empty on purpose
    }
    finishAllJobs();
    try {
      addJob(100);
    } catch (ResourceExhaustedException e) {
      fail("Should not have failed to add a job over capacity when all are finished");
    }
  }

  @Test
  public void testPurgeCount() throws Exception {
    PlanTracker tracker = new PlanTracker(10, 0, 5, mMockWorkflowTracker);
    assertEquals("tracker should be empty", 0, tracker.coordinators().size());
    fillJobTracker(tracker, 10);
    finishAllJobs(tracker);
    addJob(tracker, 100);
    assertEquals(6, tracker.coordinators().size());
  }

  @Test
  public void testRetentionTime() throws Exception {
    long retentionMs = FormatUtils.parseTimeSize("24h");
    PlanTracker tracker = new PlanTracker(10, retentionMs, -1, mMockWorkflowTracker);
    assertEquals("tracker should be empty", 0, tracker.coordinators().size());
    fillJobTracker(tracker, 10);
    finishAllJobs(tracker);
    mException.expect(ResourceExhaustedException.class);
    addJob(tracker, 100);
  }

  @Test
  public void testGetCoordinator() throws Exception {
    long jobId = addJob(100);
    assertNull("job id should not exist", mTracker.getCoordinator(-1));
    assertNotNull("job should exist", mTracker.getCoordinator(jobId));
    assertFalse("job should not be finished", mTracker.getCoordinator(jobId).isJobFinished());
    finishAllJobs();
    assertTrue("job should be finished", mTracker.getCoordinator(jobId).isJobFinished());
    assertEquals("finished should be of size 1", 1, ((Queue) Whitebox.getInternalState(mTracker,
        "mFinished")).size());
  }

  private long addJob(int sleepTimeMs) throws Exception {
    return addJob(mTracker, sleepTimeMs);
  }

  private long addJob(PlanTracker tracker, int sleepTimeMs) throws Exception {
    long jobId = mJobIdGenerator.getNewJobId();
    tracker.run(new SleepJobConfig(sleepTimeMs), mCommandManager,
        mMockJobServerContext, mWorkers, jobId);
    return jobId;
  }

  private void fillJobTracker(long nJobs) throws Exception {
    fillJobTracker(mTracker, nJobs);
  }

  private void fillJobTracker(PlanTracker tracker, long nJobs) throws Exception {
    int initial = tracker.coordinators().size();
    for (int i = 1; i <= nJobs; i++) {
      addJob(tracker, 100);
      int expectedCount = initial + i;
      assertEquals(String.format("tracker should have %d job(s)", expectedCount), expectedCount,
          tracker.coordinators().size());
      assertEquals(String.format("tracker should have %d job ids", expectedCount), expectedCount,
          tracker.list().size());
    }
  }

  private void finishAllJobs() {
    // Put all jobs in a failed state
    finishAllJobs(mTracker);
  }

  private void finishAllJobs(PlanTracker tracker) {
    // Put all jobs in a failed state
    tracker.coordinators().forEach(c -> c.setJobAsFailed("TestError", "failed for test"));
  }
}
