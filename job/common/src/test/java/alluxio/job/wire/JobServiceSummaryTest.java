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

import com.google.common.collect.Maps;
import org.junit.Assert;
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

    jobInfos.add(createJobInfo(1, Status.FAILED, 10003L));
    jobInfos.add(createJobInfo(2, Status.COMPLETED, 9998L));
    jobInfos.add(createJobInfo(3, Status.COMPLETED, 9999L));
    jobInfos.add(createJobInfo(4, Status.FAILED, 9997L));
    jobInfos.add(createJobInfo(5, Status.RUNNING, 10000L));
    jobInfos.add(createJobInfo(6, Status.FAILED, 10002L));

    mSummary = new JobServiceSummary(jobInfos);
  }

  @Test
  public void testJobServiceSummaryBuilder() {

    Collection<StatusSummary> summaryPerStatus = mSummary.getSummaryPerStatus();

    Assert.assertEquals("Unexpected length of summaryPerStatus",
            Status.values().length, summaryPerStatus.size());

    Map<Status, Long> groupByStatus = Maps.newHashMap();

    for (StatusSummary statusSummary : summaryPerStatus) {
      groupByStatus.put(statusSummary.getStatus(), statusSummary.getCount());
    }

    Assert.assertEquals("Unexpected length after grouping by status, "
                    + "perhaps there were duplicate status in StatusSummary",
            Status.values().length, summaryPerStatus.size());

    for (Status status : groupByStatus.keySet()) {
      long count = groupByStatus.get(status);
      switch (status) {
        case COMPLETED:
          Assert.assertEquals("COMPLETED count unexpected", 2L, count);
          break;
        case FAILED:
          Assert.assertEquals("FAILED count unexpected", 3L, count);
          break;
        case RUNNING:
          Assert.assertEquals("RUNNING count unexpected", 1L, count);
          break;
        default:
          Assert.assertEquals("Unexpected status having count value: " + status, 0L, count);
      }
    }
  }

  @Test
  public void testRecentActivities() {
    Collection<JobInfo> recentActivities = mSummary.getRecentActivities();

    Assert.assertEquals("Unexpected length of recent activities", 6, recentActivities.size());

    JobInfo[] recentActvitiesArray = new JobInfo[6];

    recentActivities.toArray(recentActvitiesArray);

    Assert.assertEquals(1, recentActvitiesArray[0].getJobId());
    Assert.assertEquals(6, recentActvitiesArray[1].getJobId());
    Assert.assertEquals(5, recentActvitiesArray[2].getJobId());
    Assert.assertEquals(3, recentActvitiesArray[3].getJobId());
    Assert.assertEquals(2, recentActvitiesArray[4].getJobId());
    Assert.assertEquals(4, recentActvitiesArray[5].getJobId());
  }

  @Test
  public void testRecentFailures() {
    Collection<JobInfo> recentFailures = mSummary.getRecentFailures();

    Assert.assertEquals("Unexpected length of last activities", 3, recentFailures.size());

    JobInfo[] recentFailuresArray = new JobInfo[3];

    recentFailures.toArray(recentFailuresArray);

    Assert.assertEquals(1, recentFailuresArray[0].getJobId());
    Assert.assertEquals(6, recentFailuresArray[1].getJobId());
    Assert.assertEquals(4, recentFailuresArray[2].getJobId());
  }

  private JobInfo createJobInfo(int id, Status status, long lastStatusChangeMs) {
    JobInfo jobInfo = new JobInfo();

    jobInfo.setJobId(id);
    jobInfo.setStatus(status);

    jobInfo.setLastStatusChangeMs(lastStatusChangeMs);

    return jobInfo;
  }
}
