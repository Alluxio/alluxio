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
import alluxio.RuntimeConstants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.security.authentication.AuthType;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.AlluxioWorkerInfo;
import alluxio.wire.Capacity;
import alluxio.worker.AlluxioWorkerRestServiceHandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link AlluxioWorkerRestServiceHandler}.
 */
public final class AlluxioWorkerRestApiTest extends RestApiTest {

  // TODO(chaomin): Rest API integration tests are only run in NOSASL mode now. Need to
  // fix the test setup in SIMPLE mode.
  @ClassRule
  public static LocalAlluxioClusterResource sResource = new LocalAlluxioClusterResource.Builder()
      .setProperty(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false")
      .setProperty(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName())
      .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, "1KB").build();

  @Rule
  public TestRule mResetRule = sResource.getResetResource();

  @Before
  public void before() {
    mHostname = sResource.get().getHostname();
    mPort = sResource.get().getWorkerProcess().getWebLocalPort();
    mServicePrefix = AlluxioWorkerRestServiceHandler.SERVICE_PREFIX;
  }

  private AlluxioWorkerInfo getInfo() throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.GET_INFO),
            NO_PARAMS, HttpMethod.GET, null).call();
    AlluxioWorkerInfo info = new ObjectMapper().readValue(result, AlluxioWorkerInfo.class);
    return info;
  }

  @Test
  public void getCapacity() throws Exception {
    long total = ServerConfiguration.getBytes(PropertyKey.WORKER_RAMDISK_SIZE);
    Capacity capacity = getInfo().getCapacity();
    Assert.assertEquals(total, capacity.getTotal());
    Assert.assertEquals(0, capacity.getUsed());
  }

  @Test
  public void getConfiguration() throws Exception {
    ServerConfiguration.set(PropertyKey.METRICS_CONF_FILE, "abc");
    Assert.assertEquals("abc",
        getInfo().getConfiguration().get(PropertyKey.METRICS_CONF_FILE.toString()));
  }

  @Test
  public void getMetrics() throws Exception {
    Assert.assertEquals(Long.valueOf(0),
        getInfo().getMetrics().get(MetricsSystem.getMetricName(
            MetricKey.MASTER_COMPLETE_FILE_OPS.getName())));
  }

  @Test
  public void getRpcAddress() throws Exception {
    Assert.assertTrue(
        NetworkAddressUtils.parseInetSocketAddress(getInfo().getRpcAddress()).getPort() > 0);
  }

  @Test
  public void getStartTimeMs() throws Exception {
    Assert.assertTrue(getInfo().getStartTimeMs() > 0);
  }

  @Test
  public void getTierCapacity() throws Exception {
    long total = ServerConfiguration.getBytes(PropertyKey.WORKER_RAMDISK_SIZE);
    Capacity capacity = getInfo().getTierCapacity().get(Constants.MEDIUM_MEM);
    Assert.assertEquals(total, capacity.getTotal());
    Assert.assertEquals(0, capacity.getUsed());
  }

  @Test
  public void getTierPaths() throws Exception {
    Assert.assertTrue(getInfo().getTierPaths().containsKey(Constants.MEDIUM_MEM));
  }

  @Test
  public void getUptimeMs() throws Exception {
    Assert.assertTrue(getInfo().getUptimeMs() > 0);
  }

  @Test
  public void getVersion() throws Exception {
    Assert.assertEquals(RuntimeConstants.VERSION, getInfo().getVersion());
  }
}
