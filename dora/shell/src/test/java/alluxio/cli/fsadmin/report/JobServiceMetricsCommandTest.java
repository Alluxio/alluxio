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

import alluxio.client.job.JobMasterClient;
import alluxio.grpc.BuildVersion;
import alluxio.grpc.JobMasterStatus;
import alluxio.grpc.NetAddress;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.JobServiceSummary;
import alluxio.job.wire.JobWorkerHealth;
import alluxio.job.wire.PlanInfo;
import alluxio.job.wire.Status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
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
    String startTimeStr = String.valueOf(now);
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

    jobInfos.add(new PlanInfo(1, "Test1", Status.RUNNING, 1547697600000L, null));
    jobInfos.add(new PlanInfo(2, "Test2", Status.FAILED, 1547699415000L, null));

    Mockito.when(mJobMasterClient.getJobServiceSummary())
            .thenReturn(new JobServiceSummary(jobInfos));

    new JobServiceMetricsCommand(mJobMasterClient, mPrintStream, "yyyyMMdd-HHmmss").run();

    String output = new String(mOutputStream.toByteArray(), StandardCharsets.UTF_8);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree(output);

    // Master Status Section
    JsonNode masterStatuses = jsonNode.get("masterStatus");
    assertEquals(startTimeStr, masterStatuses.get(0).get("startTime").asText());
    assertEquals("abcdef", masterStatuses.get(0).get("revision").asText());
    assertEquals("19998", masterStatuses.get(0).get("port").asText());
    assertEquals("master-node-1", masterStatuses.get(0).get("host").asText());
    assertEquals("alluxio-version-2.9", masterStatuses.get(0).get("version").asText());
    assertEquals("PRIMARY", masterStatuses.get(0).get("state").asText());
    assertEquals(startTimeStr, masterStatuses.get(1).get("startTime").asText());
    assertEquals("abcdef", masterStatuses.get(1).get("revision").asText());
    assertEquals("19998", masterStatuses.get(1).get("port").asText());
    assertEquals("master-node-0", masterStatuses.get(1).get("host").asText());
    assertEquals("alluxio-version-2.10", masterStatuses.get(1).get("version").asText());
    assertEquals("STANDBY", masterStatuses.get(1).get("state").asText());
    assertEquals(startTimeStr, masterStatuses.get(2).get("startTime").asText());
    assertEquals("bcdefg", masterStatuses.get(2).get("revision").asText());
    assertEquals("19998", masterStatuses.get(2).get("port").asText());
    assertEquals("master-node-2", masterStatuses.get(2).get("host").asText());
    assertEquals("alluxio-version-2.10", masterStatuses.get(2).get("version").asText());
    assertEquals("STANDBY", masterStatuses.get(2).get("state").asText());

    // Worker Health Section
    JsonNode workerHealth = jsonNode.get("workerHealth");
    assertEquals("ac6a0616", workerHealth.get(0).get("revision").asText());
    assertEquals("2", workerHealth.get(0).get("activeTasks").asText());
    assertEquals("1.2", workerHealth.get(0).get("loadAverage").get(0).asText());
    assertEquals("0.9", workerHealth.get(0).get("loadAverage").get(1).asText());
    assertEquals("0.7", workerHealth.get(0).get("loadAverage").get(2).asText());
    assertEquals("10", workerHealth.get(0).get("taskPoolSize").asText());
    assertEquals("2", workerHealth.get(0).get("unfinishedTasks").asText());
    assertEquals("testHost", workerHealth.get(0).get("host").asText());
    assertEquals("2.10.0-SNAPSHOT", workerHealth.get(0).get("version").asText());

    // Group By Status
    JsonNode statusSummary = jsonNode.get("statusSummary");
    assertEquals("CREATED", statusSummary.get(0).get("status").asText());
    assertEquals("0", statusSummary.get(0).get("count").asText());
    assertEquals("CANCELED", statusSummary.get(1).get("status").asText());
    assertEquals("0", statusSummary.get(1).get("count").asText());
    assertEquals("FAILED", statusSummary.get(2).get("status").asText());
    assertEquals("1", statusSummary.get(2).get("count").asText());
    assertEquals("RUNNING", statusSummary.get(3).get("status").asText());
    assertEquals("1", statusSummary.get(3).get("count").asText());
    assertEquals("COMPLETED", statusSummary.get(4).get("status").asText());
    assertEquals("0", statusSummary.get(4).get("count").asText());

    // Top 10
    JsonNode recentModifiedJobs = jsonNode.get("recentModifiedJobs");
    assertEquals("2", recentModifiedJobs.get(0).get("id").asText());
    assertEquals("FAILED", recentModifiedJobs.get(0).get("status").asText());
    assertEquals("1547699415000", recentModifiedJobs.get(0).get("lastUpdatedTime").asText());
    assertEquals("Test2", recentModifiedJobs.get(0).get("name").asText());
    assertEquals("1", recentModifiedJobs.get(1).get("id").asText());
    assertEquals("RUNNING", recentModifiedJobs.get(1).get("status").asText());
    assertEquals("1547697600000", recentModifiedJobs.get(1).get("lastUpdatedTime").asText());
    assertEquals("Test1", recentModifiedJobs.get(1).get("name").asText());

    JsonNode recentFailedJobs = jsonNode.get("recentFailedJobs");
    assertEquals("2", recentFailedJobs.get(0).get("id").asText());
    assertEquals("FAILED", recentFailedJobs.get(0).get("status").asText());
    assertEquals("1547699415000", recentFailedJobs.get(0).get("lastUpdatedTime").asText());
    assertEquals("Test2", recentFailedJobs.get(0).get("name").asText());

    JsonNode longestRunningJobs = jsonNode.get("longestRunningJobs");
    assertEquals("1", longestRunningJobs.get(0).get("id").asText());
    assertEquals("RUNNING", longestRunningJobs.get(0).get("status").asText());
    assertEquals("1547697600000", longestRunningJobs.get(0).get("lastUpdatedTime").asText());
    assertEquals("Test1", longestRunningJobs.get(0).get("name").asText());
  }
}
