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

package alluxio.job.meta;

import alluxio.job.JobConfig;
import alluxio.job.TestPlanConfig;
import alluxio.job.plan.meta.PlanInfo;
import alluxio.job.wire.Status;
import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.Test;

public final class PlanInfoTest {
  @Test
  public void compare() {
    JobConfig jobConfig = new TestPlanConfig("unused");
    PlanInfo a = new PlanInfo(0L, jobConfig, null);
    CommonUtils.sleepMs(1);
    PlanInfo b = new PlanInfo(0L, jobConfig, null);
    Assert.assertEquals(-1, a.compareTo(b));
    b.setStatus(Status.RUNNING);
    CommonUtils.sleepMs(1);
    a.setStatus(Status.RUNNING);
    Assert.assertEquals(1, a.compareTo(b));
    a.setStatus(Status.COMPLETED);
    CommonUtils.sleepMs(1);
    b.setStatus(Status.COMPLETED);
    Assert.assertEquals(-1, a.compareTo(b));
  }

  @Test
  public void callback() {
    final String result = "I was here!";
    JobConfig jobConfig = new TestPlanConfig("unused");
    PlanInfo a = new PlanInfo(0L, jobConfig, jobInfo -> jobInfo.setResult(result));
    a.setStatus(Status.COMPLETED);
    Assert.assertEquals(result, a.getResult());
  }
}
