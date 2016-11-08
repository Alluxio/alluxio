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

package alluxio.master;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.metrics.MetricsSystem;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.thrift.WorkerInfo;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link AlluxioMasterRestServiceHandler}.
 */
public final class AlluxioMasterRestApiTest extends RestApiTest {

  @Before
  public void before() {
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getMaster().getInternalMaster().getWebLocalPort();
    mServicePrefix = AlluxioMasterRestServiceHandler.SERVICE_PREFIX;

    MetricsSystem.resetAllCounters();
  }

  @Test
  public void getCapacityBytes() throws Exception {
    long memorySize = Configuration.getBytes(PropertyKey.WORKER_MEMORY_SIZE);
    new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_CAPACITY_BYTES),
        NO_PARAMS, HttpMethod.GET, memorySize).run();
  }

  @Test
  public void getUsedBytes() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_USED_BYTES),
        NO_PARAMS, HttpMethod.GET, 0).run();
  }

  @Test
  public void getFreeBytes() throws Exception {
    long freeBytes = Configuration.getBytes(PropertyKey.WORKER_MEMORY_SIZE);
    new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_FREE_BYTES),
        NO_PARAMS, HttpMethod.GET, freeBytes).run();
  }

  @Test
  public void getWorkerCount() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_WORKER_COUNT),
        NO_PARAMS, HttpMethod.GET, 1).run();
  }

  @Test
  public void getWorkerInfoList() throws Exception {
    String result = new TestCase(mHostname, mPort,
        getEndpoint(AlluxioMasterRestServiceHandler.GET_WORKER_INFO_LIST), NO_PARAMS,
        HttpMethod.GET, null).call();
    WorkerInfo[] workerInfos = new ObjectMapper().readValue(result, WorkerInfo[].class);
    Assert.assertEquals(1, workerInfos.length);
    WorkerInfo workerInfo = workerInfos[0];
    Assert.assertEquals(0, workerInfo.getUsedBytes());
    long bytes = Configuration.getBytes(PropertyKey.WORKER_MEMORY_SIZE);
    Assert.assertEquals(bytes, workerInfo.getCapacityBytes());
  }

  @Test
  public void getConfiguration() throws Exception {
    Configuration.set(PropertyKey.METRICS_CONF_FILE, "abc");
    String result = new TestCase(mHostname, mPort,
        getEndpoint(AlluxioMasterRestServiceHandler.GET_CONFIGURATION), NO_PARAMS, HttpMethod.GET,
        null).call();
    @SuppressWarnings("unchecked")
    Map<String, String> config =
        (Map<String, String>) new ObjectMapper().readValue(result, Map.class);
    Assert.assertEquals("abc", config.get(PropertyKey.METRICS_CONF_FILE.toString()));
  }

  @Test
  public void getRpcAddress() throws Exception {
    // Don't check the exact value, which could differ between systems.
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_RPC_ADDRESS),
            NO_PARAMS, HttpMethod.GET, null).call();
    Assert.assertTrue(
        result.contains(String.valueOf(NetworkAddressUtils.getPort(ServiceType.MASTER_RPC))));
  }

  @Test
  public void getMetrics() throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_METRICS),
            NO_PARAMS, HttpMethod.GET, null).call();
    @SuppressWarnings("unchecked")
    Map<String, Long> metrics = new ObjectMapper().readValue(result,
        new TypeReference<Map<String, Long>>() {});

    Assert.assertEquals(Long.valueOf(0), metrics.get("master.CompleteFileOps"));
  }

  @Test
  public void getStartTimeMs() throws Exception {
    String startTime = new TestCase(mHostname, mPort,
        getEndpoint(AlluxioMasterRestServiceHandler.GET_START_TIME_MS), NO_PARAMS, HttpMethod.GET,
        null).call();
    Assert.assertTrue(Long.valueOf(startTime) > 0);
  }

  @Test
  public void getUptimeMs() throws Exception {
    String uptime =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_UPTIME_MS),
            NO_PARAMS, HttpMethod.GET, null).call();

    Assert.assertTrue(Long.valueOf(uptime) > 0);
  }

  @Test
  public void getVersion() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_VERSION),
        NO_PARAMS, HttpMethod.GET, RuntimeConstants.VERSION).run();
  }

  @Test
  public void getUfsCapacityBytes() throws Exception {
    String ufsCapacity = new TestCase(mHostname, mPort,
        getEndpoint(AlluxioMasterRestServiceHandler.GET_UFS_CAPACITY_BYTES), NO_PARAMS,
        HttpMethod.GET, null).call();

    // Capacity should be greater than 0, or -1 which means capacity is not applicable for the
    // under storage (ie. for an object store in the cloud)
    Assert.assertTrue(Long.valueOf(ufsCapacity) > 0 || Long.valueOf(ufsCapacity) == -1);
  }

  @Test
  public void getUfsUsedBytes() throws Exception {
    // Don't check the exact value, which could differ between systems.
    new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_UFS_USED_BYTES),
        NO_PARAMS, HttpMethod.GET, null).call();
  }

  @Test
  public void getUfsFreeBytes() throws Exception {
    String ufsFreeBytes = new TestCase(mHostname, mPort,
        getEndpoint(AlluxioMasterRestServiceHandler.GET_UFS_FREE_BYTES), NO_PARAMS, HttpMethod.GET,
        null).call();

    // Free space should be greater than 0, or -1 which means free space is not applicable for the
    // under storage (ie. for an object store in the cloud)
    Assert.assertTrue(Long.valueOf(ufsFreeBytes) > 0 || Long.valueOf(ufsFreeBytes) == -1);
  }

  @Test
  public void getCapacityBytesOnTiers() throws Exception {
    Long memorySize = Configuration.getLong(PropertyKey.WORKER_MEMORY_SIZE);
    new TestCase(mHostname, mPort,
        getEndpoint(AlluxioMasterRestServiceHandler.GET_CAPACITY_BYTES_ON_TIERS), NO_PARAMS,
        HttpMethod.GET, ImmutableMap.of("MEM", memorySize)).run();
  }

  @Test
  public void getUsedBytesOnTiers() throws Exception {
    new TestCase(mHostname, mPort,
        getEndpoint(AlluxioMasterRestServiceHandler.GET_USED_BYTES_ON_TIERS), NO_PARAMS,
        HttpMethod.GET, ImmutableMap.of("MEM", 0)).run();
  }
}
