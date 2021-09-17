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

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.grpc.ListAllPOptions;
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
    Assert.assertEquals(1,
        mJobMaster.list(ListAllPOptions.getDefaultInstance()).size());
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
  public void list() throws Exception {
    List<Long> empty = Lists.newArrayList();
    Map<String, String> params = Maps.newHashMap();
    params.put("name", "");
    params.put("status", "");
    new TestCase(mHostname, mPort, getEndpoint(ServiceConstants.LIST), params,
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
