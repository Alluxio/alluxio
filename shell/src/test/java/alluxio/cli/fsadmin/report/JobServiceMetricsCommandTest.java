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

package alluxio.cli.fsadmin.report;

import alluxio.client.job.JobMasterClient;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.JobServiceSummary;
import alluxio.job.wire.Status;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;

public class JobServiceMetricsCommandTest {

  private JobMasterClient mJobMasterClient;
  private ByteArrayOutputStream mOutputStream;
  private PrintStream mPrintStream;

  @Before
  public void before() throws IOException {
    mJobMasterClient = Mockito.mock(JobMasterClient.class);

    mOutputStream = new ByteArrayOutputStream();
    mPrintStream = new PrintStream(mOutputStream, true, "utf-8");
  }

  @After
  public void after() {
    mPrintStream.close();
  }

  @Test
  public void testBasic() throws IOException {

    Collection<JobInfo> jobInfos = new ArrayList<>();

    jobInfos.add(createJobInfoWithStatus(Status.RUNNING));
    jobInfos.add(createJobInfoWithStatus(Status.CANCELED));

    Mockito.when(mJobMasterClient.getJobServiceSummary())
            .thenReturn(new JobServiceSummary(jobInfos));

    new JobServiceMetricsCommand(mJobMasterClient, mPrintStream).run();

    String output = new String(mOutputStream.toByteArray(), StandardCharsets.UTF_8);

    Assert.assertEquals("Status: CREATED   Count: 0\n"
      + "Status: CANCELED  Count: 1\n"
      + "Status: FAILED    Count: 0\n"
      + "Status: RUNNING   Count: 1\n"
      + "Status: COMPLETED Count: 0\n", output);
  }

  private JobInfo createJobInfoWithStatus(Status status) {
    JobInfo jobInfo = new JobInfo();
    jobInfo.setStatus(status);
    return jobInfo;
  }
}
