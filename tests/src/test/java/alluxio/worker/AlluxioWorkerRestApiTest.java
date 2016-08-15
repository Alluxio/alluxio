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
import alluxio.Constants;
import alluxio.RuntimeConstants;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link AlluxioWorkerRestServiceHandler}.
 */
public final class AlluxioWorkerRestApiTest extends RestApiTest {

  @Before
  public void before() {
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getWorker().getWebLocalPort();
    mServicePrefix = AlluxioWorkerRestServiceHandler.SERVICE_PREFIX;
  }

  @Test
  public void getRpcAddressTest() throws Exception {
    // Don't check the exact value, which could differ between systems.
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.GET_RPC_ADDRESS),
            NO_PARAMS, HttpMethod.GET, null).call();
    Assert.assertTrue(
        result.contains(String.valueOf(NetworkAddressUtils.getPort(ServiceType.WORKER_RPC))));
  }

  @Test
  public void getCapacityBytesTest() throws Exception {
    long memorySize = Configuration.getBytes(Constants.WORKER_MEMORY_SIZE);
    new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.GET_CAPACITY_BYTES),
        NO_PARAMS, HttpMethod.GET, memorySize).run();
  }

  /** Tests worker's REST API for getting alluxio configuration.
   *
   * @throws Exception when any error happens
   */
  @Test
  public void getConfigurationTest() throws Exception {
    Configuration.set("alluxio.testkey", "abc");
    String result = new TestCase(mHostname, mPort,
        getEndpoint(AlluxioWorkerRestServiceHandler.GET_CONFIGURATION), NO_PARAMS, HttpMethod.GET,
        null).call();
    @SuppressWarnings("unchecked")
    Map<String, String> config =
        (Map<String, String>) new ObjectMapper().readValue(result, Map.class);
    Assert.assertEquals("abc", config.get("alluxio.testkey"));
  }

  @Test
  public void getUsedBytesTest() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.GET_USED_BYTES),
        NO_PARAMS, HttpMethod.GET, 0).run();
  }

  @Test
  public void getMetricsTest() throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.GET_METRICS),
            NO_PARAMS, HttpMethod.GET, null).call();
    @SuppressWarnings("unchecked")
    Map<String, Long> metrics = (Map<String, Long>) new ObjectMapper().readValue(result,
        new TypeReference<Map<String, Long>>() {});

    String blocksAccessedMetricName =
        WorkerContext.getWorkerSource().getName() + "." + WorkerSource.BLOCKS_ACCESSED;
    Assert.assertTrue(metrics.get(blocksAccessedMetricName) >= 0);
  }

  @Test
  public void getVersionTest() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.GET_VERSION),
        NO_PARAMS, HttpMethod.GET, RuntimeConstants.VERSION).run();
  }

  @Test
  public void getCapacityBytesOnTiersTest() throws Exception {
    Long memorySize = Configuration.getLong(Constants.WORKER_MEMORY_SIZE);
    new TestCase(mHostname, mPort,
        getEndpoint(AlluxioWorkerRestServiceHandler.GET_CAPACITY_BYTES_ON_TIERS), NO_PARAMS,
        HttpMethod.GET, ImmutableMap.of("MEM", memorySize)).run();
  }

  @Test
  public void getUsedBytesOnTiersTest() throws Exception {
    new TestCase(mHostname, mPort,
        getEndpoint(AlluxioWorkerRestServiceHandler.GET_USED_BYTES_ON_TIERS), NO_PARAMS,
        HttpMethod.GET, ImmutableMap.of("MEM", 0)).run();
  }

  @Test
  public void getDirectoryPathsOnTiersTest() throws Exception {
    String result = new TestCase(mHostname, mPort,
        getEndpoint(AlluxioWorkerRestServiceHandler.GET_DIRECTORY_PATHS_ON_TIERS), NO_PARAMS,
        HttpMethod.GET, null).call();
    @SuppressWarnings("unchecked")
    Map<String, List<String>> pathsOnTiers = (Map<String, List<String>>) new ObjectMapper()
        .readValue(result, new TypeReference<Map<String, List<String>>>() {});
    Entry<String, List<String>> entry = Iterables.getOnlyElement(pathsOnTiers.entrySet());
    Assert.assertEquals("MEM", entry.getKey());
    String path = Iterables.getOnlyElement(entry.getValue());
    Assert.assertTrue(path.contains(Configuration.get(Constants.WORKER_DATA_FOLDER)));
  }

  @Test
  public void getStartTimeMsTest() throws Exception {
    String startTimeString = new TestCase(mHostname, mPort,
        getEndpoint(AlluxioWorkerRestServiceHandler.GET_START_TIME_MS), NO_PARAMS, HttpMethod.GET,
        null).call();
    long startTime = Long.parseLong(startTimeString);
    Assert.assertTrue(startTime > System.currentTimeMillis() - 20 * Constants.SECOND_MS);
    Assert.assertTrue(startTime <= System.currentTimeMillis());
  }

  @Test
  public void getUptimeMsTest() throws Exception {
    CommonUtils.sleepMs(1);
    String uptimeString =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.GET_UPTIME_MS),
            NO_PARAMS, HttpMethod.GET, null).call();
    long uptime = Long.parseLong(uptimeString);
    Assert.assertTrue(uptime > 0);
    Assert.assertTrue(uptime < 20 * Constants.SECOND_MS);
  }
}
