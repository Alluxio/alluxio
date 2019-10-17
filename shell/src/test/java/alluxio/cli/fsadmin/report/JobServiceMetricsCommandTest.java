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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

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
  public void testBasic() throws IOException, ParseException {

    List<JobInfo> jobInfos = new ArrayList<>();


    jobInfos.add(createJobInfo(1, Status.RUNNING, "2019-10-17 12:00:00"));
    jobInfos.add(createJobInfo(2, Status.FAILED, "2019-10-17 12:30:15"));

    Mockito.when(mJobMasterClient.getJobServiceSummary())
            .thenReturn(new JobServiceSummary(jobInfos));

    new JobServiceMetricsCommand(mJobMasterClient, mPrintStream, "MM-dd-yyyy HH:mm:ss:SSS").run();

    String output = new String(mOutputStream.toByteArray(), StandardCharsets.UTF_8);

    String[] lineByLine = output.split("\n");


    Assert.assertEquals("Status: CREATED   Count: 0", lineByLine[0]);
    Assert.assertEquals("Status: CANCELED  Count: 0", lineByLine[1]);
    Assert.assertEquals("Status: FAILED    Count: 1", lineByLine[2]);
    Assert.assertEquals("Status: RUNNING   Count: 1", lineByLine[3]);
    Assert.assertEquals("Status: COMPLETED Count: 0", lineByLine[4]);

    Assert.assertEquals("Last 10 Activities:", lineByLine[6]);
    Assert.assertEquals(
      "Timestamp: 01-17-2019 12:30:15:000       Job Id: 2                   Status: FAILED",
      lineByLine[7]);
    Assert.assertEquals(
      "Timestamp: 01-17-2019 12:00:00:000       Job Id: 1                   Status: RUNNING",
      lineByLine[8]);

    Assert.assertEquals("Last 10 Failures:", lineByLine[10]);
    Assert.assertEquals(
      "Timestamp: 01-17-2019 12:30:15:000       Job Id: 2                   Status: FAILED",
      lineByLine[11]);
  }

  private JobInfo createJobInfo(int id, Status status, String datetime) throws ParseException {
    JobInfo jobInfo = new JobInfo();

    jobInfo.setJobId(id);
    jobInfo.setStatus(status);

    long timeMillis = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss").parse(datetime).getTime();
    jobInfo.setLastStatusChangeMs(timeMillis);

    return jobInfo;
  }
}
