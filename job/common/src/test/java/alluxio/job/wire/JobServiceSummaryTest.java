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

package alluxio.job.wire;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Tests the wire format of {@link JobServiceSummary}.
 */
public class JobServiceSummaryTest {

  JobServiceSummary mSummary;

  @Before
  public void before() {
    List<JobInfo> jobInfos = new ArrayList<>();

    jobInfos.add(createPlanInfo(1, Status.FAILED, 10003L));
    jobInfos.add(createPlanInfo(2, Status.COMPLETED, 9998L));
    jobInfos.add(createPlanInfo(3, Status.COMPLETED, 9999L));
    jobInfos.add(createPlanInfo(4, Status.FAILED, 9997L));
    jobInfos.add(createPlanInfo(5, Status.RUNNING, 10000L));
    jobInfos.add(createPlanInfo(6, Status.FAILED, 10002L));
    jobInfos.add(createWorkflowInfo(7, Status.RUNNING, 9996L));

    mSummary = new JobServiceSummary(jobInfos);
  }

  @Test
  public void toProto() throws Exception {
    final JobServiceSummary summary = new JobServiceSummary(mSummary.toProto());

    assertEquals(mSummary, summary);
  }

  @Test
  public void testJobServiceSummaryBuilder() {

    Collection<StatusSummary> summaryPerStatus = mSummary.getSummaryPerStatus();

    assertEquals("Unexpected length of summaryPerStatus",
            Status.values().length, summaryPerStatus.size());

    Map<Status, Long> groupByStatus = Maps.newHashMap();

    for (StatusSummary statusSummary : summaryPerStatus) {
      groupByStatus.put(statusSummary.getStatus(), statusSummary.getCount());
    }

    assertEquals("Unexpected length after grouping by status, "
                    + "perhaps there were duplicate status in StatusSummary",
            Status.values().length, summaryPerStatus.size());

    for (Status status : groupByStatus.keySet()) {
      long count = groupByStatus.get(status);
      switch (status) {
        case COMPLETED:
          assertEquals("COMPLETED count unexpected", 2L, count);
          break;
        case FAILED:
          assertEquals("FAILED count unexpected", 3L, count);
          break;
        case RUNNING:
          assertEquals("RUNNING count unexpected", 2L, count);
          break;
        default:
          assertEquals("Unexpected status having count value: " + status, 0L, count);
      }
    }
  }

  @Test
  public void testRecentActivities() {
    Collection<JobInfo> recentActivities = mSummary.getRecentActivities();

    assertEquals("Unexpected length of recent activities", 7, recentActivities.size());

    JobInfo[] recentActvitiesArray = new JobInfo[7];

    recentActivities.toArray(recentActvitiesArray);

    assertEquals(1, recentActvitiesArray[0].getId());
    assertEquals(6, recentActvitiesArray[1].getId());
    assertEquals(5, recentActvitiesArray[2].getId());
    assertEquals(3, recentActvitiesArray[3].getId());
    assertEquals(2, recentActvitiesArray[4].getId());
    assertEquals(4, recentActvitiesArray[5].getId());
    assertEquals(7, recentActvitiesArray[6].getId());
  }

  @Test
  public void testRecentFailures() {
    Collection<JobInfo> recentFailures = mSummary.getRecentFailures();

    assertEquals("Unexpected length of last activities", 3, recentFailures.size());

    JobInfo[] recentFailuresArray = new JobInfo[3];

    recentFailures.toArray(recentFailuresArray);

    assertEquals(1, recentFailuresArray[0].getId());
    assertEquals(6, recentFailuresArray[1].getId());
    assertEquals(4, recentFailuresArray[2].getId());
  }

  @Test
  public void testLongestRunning() {
    Collection<JobInfo> longestRunning = mSummary.getLongestRunning();

    assertEquals("Unexpected length of longest running", 2, longestRunning.size());

    JobInfo[] longestRunningArray = new JobInfo[2];

    longestRunning.toArray(longestRunningArray);

    assertEquals(7, longestRunningArray[0].getId());
    assertEquals(5, longestRunningArray[1].getId());
  }

  private PlanInfo createPlanInfo(int id, Status status, long lastStatusChangeMs) {
    return new PlanInfo(id, "test", status, lastStatusChangeMs, null);
  }

  private WorkflowInfo createWorkflowInfo(int id, Status status, long lastStatusChangeMs) {
    return new WorkflowInfo(id, "name", status, lastStatusChangeMs, "", "", Lists.newArrayList());
  }
}
