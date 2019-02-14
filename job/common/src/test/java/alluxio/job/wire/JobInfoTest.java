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

import alluxio.util.CommonUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Random;

/**
 * Tests the wire format of {@link JobInfo}.
 */
public final class JobInfoTest {
  @Test
  public void json() throws Exception {
    JobInfo jobInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    JobInfo other = mapper.readValue(mapper.writeValueAsBytes(jobInfo), JobInfo.class);
    checkEquality(jobInfo, other);
  }

  public static JobInfo createRandom() {
    JobInfo result = new JobInfo();
    Random random = new Random();

    List<TaskInfo> taskInfoList = Lists.newArrayList();
    for (int i = 0; i < random.nextInt(10); i++) {
      taskInfoList.add(TaskInfoTest.createRandom());
    }
    result.setTaskInfoList(taskInfoList);
    result.setErrorMessage(CommonUtils.randomAlphaNumString(random.nextInt(10)));
    result.setJobId(random.nextLong());
    result.setResult(CommonUtils.randomAlphaNumString(random.nextInt(10)));
    result.setStatus(Status.values()[random.nextInt(Status.values().length)]);
    return result;
  }

  public void checkEquality(JobInfo a, JobInfo b) {
    Assert.assertEquals(a.getErrorMessage(), b.getErrorMessage());
    Assert.assertEquals(a.getJobId(), b.getJobId());
    Assert.assertEquals(a.getTaskInfoList(), b.getTaskInfoList());
    Assert.assertEquals(a.getResult(), b.getResult());
    Assert.assertEquals(a.getStatus(), b.getStatus());
  }
}
