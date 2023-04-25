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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.client.job.JobMasterClient;
import alluxio.grpc.BuildVersion;
import alluxio.grpc.JobMasterStatus;
import alluxio.grpc.NetAddress;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.JobServiceSummary;
import alluxio.job.wire.JobWorkerHealth;
import alluxio.job.wire.PlanInfo;
import alluxio.job.wire.Status;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
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
    long now = Instant.now().toEpochMilli();
    String startTimeStr = JobServiceMetricsCommand.DATETIME_FORMAT
        .format(Instant.ofEpochMilli(now));
    JobMasterStatus primaryMaster = JobMasterStatus.newBuilder()
        .setMasterAddress(NetAddress.newBuilder()
            .setHost("master-node-1").setRpcPort(19998).build())
        .setState("PRIMARY").setStartTime(now).setVersion(BuildVersion.newBuilder()
            .setVersion("alluxio-version-2.9").setRevision("abcdef").build()).build();
    JobMasterStatus standbyMaster1 = JobMasterStatus.newBuilder()
        .setMasterAddress(NetAddress.newBuilder()
            .setHost("master-node-0").setRpcPort(19998).build())
        .setState("STANDBY").setStartTime(now).setVersion(
            BuildVersion.newBuilder().setVersion("alluxio-version-2.10")
                .setRevision("abcdef").build()).build();
    JobMasterStatus standbyMaster2 = JobMasterStatus.newBuilder()
        .setMasterAddress(NetAddress.newBuilder()
            .setHost("master-node-2").setRpcPort(19998).build())
        .setState("STANDBY").setStartTime(now).setVersion(
            BuildVersion.newBuilder().setVersion("alluxio-version-2.10")
                .setRevision("bcdefg").build()).build();
    Mockito.when(mJobMasterClient.getAllMasterStatus())
        .thenReturn(Lists.newArrayList(primaryMaster, standbyMaster1, standbyMaster2));

    JobWorkerHealth jobWorkerHealth = new JobWorkerHealth(
        1, Lists.newArrayList(1.2, 0.9, 0.7),
            10, 2, 2, "testHost",
            BuildVersion.newBuilder()
                .setVersion("2.10.0-SNAPSHOT").setRevision("ac6a0616").build());
    Mockito.when(mJobMasterClient.getAllWorkerHealth())
        .thenReturn(Lists.newArrayList(jobWorkerHealth));

    List<JobInfo> jobInfos = new ArrayList<>();

    jobInfos.add(createJobInfo(1, "Test1", Status.RUNNING, "2019-10-17 12:00:00"));
    jobInfos.add(createJobInfo(2, "Test2", Status.FAILED, "2019-10-17 12:30:15"));

    Mockito.when(mJobMasterClient.getJobServiceSummary())
            .thenReturn(new JobServiceSummary(jobInfos));

    new JobServiceMetricsCommand(mJobMasterClient, mPrintStream, "MM-dd-yyyy HH:mm:ss:SSS").run();

    String output = new String(mOutputStream.toByteArray(), StandardCharsets.UTF_8);

    String[] lineByLine = output.split("\n");

    // Master Status Section
    assertTrue(lineByLine[0].contains("Master Address      State    Start Time       "
        + "Version                          Revision"));
    assertTrue(lineByLine[1].contains("master-node-1:19998 PRIMARY"));
    assertTrue(lineByLine[1].contains(startTimeStr));
    assertTrue(lineByLine[1].contains("alluxio-version-2.9              abcdef"));
    assertTrue(lineByLine[2].contains("master-node-0:19998 STANDBY"));
    assertTrue(lineByLine[2].contains(startTimeStr));
    assertTrue(lineByLine[2].contains("alluxio-version-2.10             abcdef"));
    assertTrue(lineByLine[3].contains("master-node-2:19998 STANDBY"));
    assertTrue(lineByLine[3].contains(startTimeStr));
    assertTrue(lineByLine[3].contains("alluxio-version-2.10             bcdefg"));

    // Worker Health Section
    assertTrue(lineByLine[5].contains("Job Worker       Version                          "
        + "Revision Task Pool Size Unfinished Tasks Active Tasks Load Avg"));
    assertTrue(lineByLine[6].contains("testHost         2.10.0-SNAPSHOT                  "
        + "ac6a0616"));
    assertTrue(lineByLine[6].contains("10             2                2            "
        + "1.2, 0.9, 0.7"));

    // Group By Status
    lineByLine = ArrayUtils.subarray(lineByLine, 8, lineByLine.length);

    assertEquals("Status: CREATED   Count: 0", lineByLine[0]);
    assertEquals("Status: CANCELED  Count: 0", lineByLine[1]);
    assertEquals("Status: FAILED    Count: 1", lineByLine[2]);
    assertEquals("Status: RUNNING   Count: 1", lineByLine[3]);
    assertEquals("Status: COMPLETED Count: 0", lineByLine[4]);
    assertEquals("", lineByLine[5]);

    // Top 10
    lineByLine = ArrayUtils.subarray(lineByLine, 6, lineByLine.length);

    assertEquals("10 Most Recently Modified Jobs:", lineByLine[0]);
    assertEquals(
        "Timestamp: 01-17-2019 12:30:15:000       Id: 2                   Name: Test2"
        + "               Status: FAILED",
        lineByLine[1]);
    assertEquals(
        "Timestamp: 01-17-2019 12:00:00:000       Id: 1                   Name: Test1"
        + "               Status: RUNNING",
        lineByLine[2]);
    assertEquals("", lineByLine[3]);
    assertEquals("10 Most Recently Failed Jobs:", lineByLine[4]);
    assertEquals(
        "Timestamp: 01-17-2019 12:30:15:000       Id: 2                   Name: Test2"
        + "               Status: FAILED",
        lineByLine[5]);
    assertEquals("", lineByLine[6]);
    assertEquals("10 Longest Running Jobs:", lineByLine[7]);
    assertEquals(
        "Timestamp: 01-17-2019 12:00:00:000       Id: 1                   Name: Test1"
            + "               Status: RUNNING",
        lineByLine[8]);
  }

  private JobInfo createJobInfo(int id, String name, Status status, String datetime)
      throws ParseException {
    long timeMillis = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss").parse(datetime).getTime();
    PlanInfo jobInfo = new PlanInfo(id, name, status, timeMillis, null);
    return jobInfo;
  }
}
