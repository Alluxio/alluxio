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
import alluxio.job.TestJobConfig;
import alluxio.job.wire.Status;
import alluxio.util.CommonUtils;

import com.google.common.base.Function;
import org.junit.Assert;
import org.junit.Test;

public final class JobInfoTest {
  @Test
  public void compare() {
    JobConfig jobConfig = new TestJobConfig("unused");
    JobInfo a = new JobInfo(0L, jobConfig, null);
    CommonUtils.sleepMs(1);
    JobInfo b = new JobInfo(0L, jobConfig, null);
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
    JobConfig jobConfig = new TestJobConfig("unused");
    JobInfo a = new JobInfo(0L, jobConfig, new Function<JobInfo, Void>() {
      @Override
      public Void apply(JobInfo jobInfo) {
        jobInfo.setResult(result);
        return null;
      }
    });
    a.setStatus(Status.COMPLETED);
    Assert.assertEquals(result, a.getResult());
  }
}
