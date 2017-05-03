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

package alluxio.worker;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.AlluxioWorkerInfo;
import alluxio.wire.Capacity;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link AlluxioWorkerRestServiceHandler}.
 */
public final class AlluxioWorkerRestApiTest extends RestApiTest {

  @Before
  public void before() {
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getWorkerProcess().getWebLocalPort();
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
    long total = Configuration.getBytes(PropertyKey.WORKER_MEMORY_SIZE);
    Capacity capacity = getInfo().getCapacity();
    Assert.assertEquals(total, capacity.getTotal());
    Assert.assertEquals(0, capacity.getUsed());
  }

  @Test
  public void getConfiguration() throws Exception {
    Configuration.set(PropertyKey.METRICS_CONF_FILE, "abc");
    Assert.assertEquals("abc",
        getInfo().getConfiguration().get(PropertyKey.METRICS_CONF_FILE.toString()));
  }

  @Test
  public void getMetrics() throws Exception {
    Assert.assertEquals(Long.valueOf(0), getInfo().getMetrics().get("master.CompleteFileOps"));
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
    long total = Configuration.getLong(PropertyKey.WORKER_MEMORY_SIZE);
    Capacity capacity = getInfo().getTierCapacity().get("MEM");
    Assert.assertEquals(total, capacity.getTotal());
    Assert.assertEquals(0, capacity.getUsed());
  }

  @Test
  public void getTierPaths() throws Exception {
    Assert.assertTrue(getInfo().getTierPaths().containsKey("MEM"));
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
