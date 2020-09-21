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

package alluxio.client.rest;

import static org.junit.Assert.assertEquals;

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.job.CrashPlanConfig;
import alluxio.job.JobConfig;
import alluxio.job.ServiceConstants;
import alluxio.job.SleepJobConfig;
import alluxio.job.wire.Status;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.job.JobMaster;
import alluxio.master.job.JobMasterClientRestServiceHandler;
import alluxio.security.authentication.AuthType;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.HttpMethod;

/**
 * Tests {@link JobMasterClientRestServiceHandler}.
 */
public final class JobMasterClientRestApiTest extends RestApiTest {
  private static final Map<String, String> NO_PARAMS = Maps.newHashMap();
  private LocalAlluxioJobCluster mJobCluster;
  private JobMaster mJobMaster;

  // TODO(chaomin): Rest API integration tests are only run in NOSASL mode now. Need to
  // fix the test setup in SIMPLE mode.
  @ClassRule
  public static LocalAlluxioClusterResource sResource = new LocalAlluxioClusterResource.Builder()
      .setProperty(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false")
      .setProperty(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName())
      .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, "1KB")
      .setProperty(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL, "10ms")
      .setProperty(PropertyKey.JOB_MASTER_FINISHED_JOB_RETENTION_TIME, "0sec")
      .build();

  @Rule
  public TestRule mResetRule = sResource.getResetResource();

  @Before
  public void before() throws Exception {
    mJobCluster = new LocalAlluxioJobCluster();
    mJobCluster.start();
    mJobMaster = mJobCluster.getMaster().getJobMaster();
    mHostname = mJobCluster.getHostname();
    mPort = mJobCluster.getMaster().getWebAddress().getPort();
    mServicePrefix = ServiceConstants.MASTER_SERVICE_PREFIX;
  }

  @After
  public void after() throws Exception {
    mJobCluster.stop();
  }

  @Test
  public void serviceName() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(ServiceConstants.SERVICE_NAME),
        NO_PARAMS, HttpMethod.GET, Constants.JOB_MASTER_CLIENT_SERVICE_NAME).run();
  }

  @Test
  public void serviceVersion() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(ServiceConstants.SERVICE_VERSION),
        NO_PARAMS, HttpMethod.GET, Constants.JOB_MASTER_CLIENT_SERVICE_VERSION).run();
  }

  @Test
  public void run() throws Exception {
    final long jobId = startJob(new SleepJobConfig(200));
    Assert.assertEquals(1, mJobMaster.list().size());
    waitForStatus(jobId, Status.COMPLETED);
  }

  @Test
  public void cancel() throws Exception {
    long jobId = startJob(new SleepJobConfig(10 * Constants.SECOND_MS));
    // Sleep to make sure the run request and the cancel request are separated by a job worker
    // heartbeat. If not, job service will not handle that case correctly.
    CommonUtils.sleepMs(30);
    Map<String, String> params = Maps.newHashMap();
    params.put("jobId", Long.toString(jobId));
    new TestCase(mHostname, mPort, getEndpoint(ServiceConstants.CANCEL),
        params, HttpMethod.POST, null).run();
    waitForStatus(jobId, Status.CANCELED);
  }

  @Test
  public void failre_history() throws Exception {
    final long jobId0 = startJob(new CrashPlanConfig("/test"));
    waitForStatus(jobId0, Status.FAILED);
    final long jobId1 = startJob(new CrashPlanConfig("/test"));
    waitForStatus(jobId1, Status.FAILED);
    final long jobId2 = startJob(new CrashPlanConfig("/test"));
    waitForStatus(jobId2, Status.FAILED);
    String result = new TestCase(mHostname, mPort,
        getEndpoint(ServiceConstants.FAILURE_HISTORY), NO_PARAMS, HttpMethod.GET, null).call();

    final ObjectMapper mapper = new ObjectMapper();
    List<Map<String, Object>> resultList = mapper.readValue(result, List.class);

    assertEquals(3, resultList.size());

    Map<String, Object> map = resultList.get(0);
    assertEquals(jobId2, map.get("id"));
    assertEquals("FAILED", map.get("status"));
    assertEquals("Crash", map.get("name"));
    assertEquals(ImmutableList.of("/test"), map.get("affectedPaths"));
    assertEquals("Task execution failed: CrashPlanConfig always crashes", map.get("errorMessage"));
    assertEquals("IllegalStateException", map.get("errorType"));

    map = resultList.get(1);
    assertEquals(jobId1, map.get("id"));

    map = resultList.get(2);
    assertEquals(jobId0, map.get("id"));

    Map<String, String> params = Maps.newHashMap();
    params.put("limit", "1");

    result = new TestCase(mHostname, mPort,
        getEndpoint(ServiceConstants.FAILURE_HISTORY), params, HttpMethod.GET, null).call();
    resultList = mapper.readValue(result, List.class);

    assertEquals(1, resultList.size());
    map = resultList.get(0);
    assertEquals(jobId2, map.get("id"));
  }

  @Test
  public void failure_history_filters() throws Exception {
    final long jobId0 = startJob(new CrashPlanConfig("/test"));
    waitForStatus(jobId0, Status.FAILED);
    final long jobId1 = startJob(new CrashPlanConfig("/test"));
    waitForStatus(jobId1, Status.FAILED);
    final long jobId2 = startJob(new CrashPlanConfig("/test"));
    waitForStatus(jobId2, Status.FAILED);

    String result = new TestCase(mHostname, mPort,
        getEndpoint(ServiceConstants.FAILURE_HISTORY), NO_PARAMS, HttpMethod.GET, null).call();

    final ObjectMapper mapper = new ObjectMapper();
    List<Map<String, Object>> resultList = mapper.readValue(result, List.class);

    assertEquals(3, resultList.size());

    Map<String, Object> map = resultList.get(0);
    assertEquals(jobId2, map.get("id"));
    final Long lastUpdated2 = (Long) map.get("lastUpdated");

    map = resultList.get(1);
    assertEquals(jobId1, map.get("id"));
    final Long lastUpdated1 = (Long) map.get("lastUpdated");

    map = resultList.get(2);
    assertEquals(jobId0, map.get("id"));
    final Long lastUpdated0 = (Long) map.get("lastUpdated");

    Map<String, String> params = Maps.newHashMap();
    params.put("before", Long.toString(lastUpdated2));
    params.put("after", Long.toString(lastUpdated0));

    result = new TestCase(mHostname, mPort,
        getEndpoint(ServiceConstants.FAILURE_HISTORY), params, HttpMethod.GET, null).call();
    resultList = mapper.readValue(result, List.class);

    assertEquals(1, resultList.size());
    assertEquals(jobId1, resultList.get(0).get("id"));

    params.put("after", Long.toString(lastUpdated0 - 1));
    result = new TestCase(mHostname, mPort,
        getEndpoint(ServiceConstants.FAILURE_HISTORY), params, HttpMethod.GET, null).call();
    resultList = mapper.readValue(result, List.class);
    assertEquals(2, resultList.size());
    assertEquals(jobId1, resultList.get(0).get("id"));
    assertEquals(jobId0, resultList.get(1).get("id"));

    params.put("before", Long.toString(lastUpdated2 + 1));
    result = new TestCase(mHostname, mPort,
        getEndpoint(ServiceConstants.FAILURE_HISTORY), params, HttpMethod.GET, null).call();
    resultList = mapper.readValue(result, List.class);
    assertEquals(3, resultList.size());
  }

  @Test
  public void list() throws Exception {
    List<Long> empty = Lists.newArrayList();
    new TestCase(mHostname, mPort, getEndpoint(ServiceConstants.LIST), NO_PARAMS,
        HttpMethod.GET, empty).run();
  }

  @Test
  public void getStatus() throws Exception {
    JobConfig config = new SleepJobConfig(Constants.SECOND_MS);
    final long jobId = startJob(config);
    Map<String, String> params = Maps.newHashMap();
    params.put("jobId", Long.toString(jobId));

    TestCaseOptions options = TestCaseOptions.defaults().setPrettyPrint(true);
    String result = new TestCase(mHostname, mPort, getEndpoint(ServiceConstants.GET_STATUS),
        params, HttpMethod.GET, null, options).call();
    TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>(){};
    HashMap<String, Object> jobInfo = new ObjectMapper().readValue(result, typeRef);
    Assert.assertEquals(jobId, jobInfo.get("id"));
    Assert.assertEquals(1, ((Collection) jobInfo.get("children")).size());
  }

  private long startJob(JobConfig config) throws Exception {
    TestCaseOptions options = TestCaseOptions.defaults().setBody(config);
    String result = new TestCase(mHostname, mPort, getEndpoint(ServiceConstants.RUN),
        NO_PARAMS, HttpMethod.POST, null, options).call();
    return new ObjectMapper().readValue(result, Long.TYPE);
  }

  private void waitForStatus(final long jobId, final Status status)
      throws InterruptedException, TimeoutException {
    CommonUtils.waitFor("Waiting for job status", () -> {
      try {
        return mJobMaster.getStatus(jobId).getStatus() == status;
      } catch (Exception e) {
        Throwables.propagate(e);
      }
      return null;
    }, WaitForOptions.defaults().setTimeoutMs(10 * Constants.SECOND_MS));
  }
}
