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

import alluxio.IntegrationTestUtils;
import alluxio.RuntimeConstants;
import alluxio.WorkerStorageTierAssoc;
import alluxio.master.MasterContext;
import alluxio.metrics.MetricsSystem;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.util.CommonUtils;
import alluxio.worker.block.BlockStoreMeta;
import alluxio.worker.block.BlockWorker;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link AlluxioWorkerRestServiceHandler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioWorker.class, BlockWorker.class, BlockStoreMeta.class})
public final class AlluxioWorkerRestApiTest extends RestApiTest {
  private AlluxioWorker mWorker;
  private BlockStoreMeta mStoreMeta;

  @Before
  public void before() {
    mWorker = PowerMockito.spy(mResource.get().getWorker());
    Whitebox.setInternalState(AlluxioWorker.class, "sAlluxioWorker", mWorker);
    BlockWorker blockWorker = PowerMockito.mock(BlockWorker.class);
    Whitebox.setInternalState(mWorker, "mBlockWorker", blockWorker);
    mStoreMeta = PowerMockito.mock(BlockStoreMeta.class);
    Mockito.doReturn(mStoreMeta).when(blockWorker).getStoreMeta();
    Mockito.doReturn(blockWorker).when(mWorker).getBlockWorker();
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getWorker().getWebLocalPort();
    mServicePrefix = AlluxioWorkerRestServiceHandler.SERVICE_PREFIX;
  }

  @Test
  public void getRpcAddressTest() throws Exception {
    Random random = new Random();
    InetSocketAddress address = new InetSocketAddress(IntegrationTestUtils.randomString(),
        random.nextInt(8080) + 1);
    Mockito.doReturn(address).when(mWorker).getWorkerAddress();

    new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.GET_RPC_ADDRESS),
        NO_PARAMS, HttpMethod.GET, address.toString()).run();

    Mockito.verify(mWorker).getWorkerAddress();
  }

  @Test
  public void getCapacityBytesTest() throws Exception {
    Random random = new Random();
    long capacityBytes = random.nextLong();
    Mockito.doReturn(capacityBytes).when(mStoreMeta).getCapacityBytes();

    new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.GET_CAPACITY_BYTES),
        NO_PARAMS, HttpMethod.GET, capacityBytes).run();
  }

  @Test
  public void getUsedBytesTest() throws Exception {
    Random random = new Random();
    long usedBytes = random.nextLong();
    Mockito.doReturn(usedBytes).when(mStoreMeta).getUsedBytes();

    new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.GET_USED_BYTES),
        NO_PARAMS, HttpMethod.GET, usedBytes).run();
  }

  @Test
  public void getMetricsTest() throws Exception {
    // Mock worker metrics system.
    MetricRegistry metricRegistry = PowerMockito.mock(MetricRegistry.class);
    MetricsSystem metricsSystem = PowerMockito.mock(MetricsSystem.class);
    Mockito.doReturn(metricRegistry).when(metricsSystem).getMetricRegistry();
    Mockito.doReturn(metricsSystem).when(mWorker).getWorkerMetricsSystem();

    // Generate random metrics.
    Random random = new Random();
    SortedMap<String, Long> metricsMap = new TreeMap<>();
    metricsMap.put(IntegrationTestUtils.randomString(), random.nextLong());
    metricsMap.put(IntegrationTestUtils.randomString(), random.nextLong());
    String blocksCachedProperty = CommonUtils.argsToString(".",
        WorkerContext.getWorkerSource().getName(), WorkerSource.BLOCKS_CACHED);
    Integer blocksCached = random.nextInt();
    metricsMap.put(blocksCachedProperty, blocksCached.longValue());

    // Mock counters.
    SortedMap<String, Counter> counters = new TreeMap<>();
    for (Map.Entry<String, Long> entry : metricsMap.entrySet()) {
      Counter counter = new Counter();
      counter.inc(entry.getValue());
      counters.put(entry.getKey(), counter);
    }
    Mockito.doReturn(counters).when(metricRegistry).getCounters();

    // Mock gauges.
    Gauge<?> blocksCachedGauge = PowerMockito.mock(Gauge.class);
    Mockito.doReturn(blocksCached).when(blocksCachedGauge).getValue();
    SortedMap<String, Gauge<?>> gauges = new TreeMap<>();
    gauges.put(blocksCachedProperty, blocksCachedGauge);
    Mockito.doReturn(gauges).when(metricRegistry).getGauges();

    new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.GET_METRICS),
        NO_PARAMS, HttpMethod.GET, metricsMap).run();

    Mockito.verify(metricRegistry).getCounters();
    Mockito.verify(metricRegistry).getGauges();
    Mockito.verify(blocksCachedGauge).getValue();
  }

  @Test
  public void getVersionTest() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.GET_VERSION),
        NO_PARAMS, HttpMethod.GET, RuntimeConstants.VERSION).run();
  }

  @Test
  public void getCapacityBytesOnTiersTest() throws Exception {
    Random random = new Random();
    WorkerStorageTierAssoc tierAssoc = new WorkerStorageTierAssoc(MasterContext.getConf());
    int nTiers = tierAssoc.size();
    // LinkedHashMap keeps keys in the serialized json object in the insertion order, the insertion
    // order is from smaller tier ordinal to larger ones.
    LinkedHashMap<String, Long> capacityBytesOnTiers = new LinkedHashMap<>();
    for (int ordinal = 0; ordinal < nTiers; ordinal++) {
      capacityBytesOnTiers.put(tierAssoc.getAlias(ordinal), random.nextLong());
    }
    Mockito.doReturn(capacityBytesOnTiers).when(mStoreMeta).getCapacityBytesOnTiers();

    new TestCase(mHostname, mPort,
        getEndpoint(AlluxioWorkerRestServiceHandler.GET_CAPACITY_BYTES_ON_TIERS), NO_PARAMS,
        HttpMethod.GET, capacityBytesOnTiers).run();

    Mockito.verify(mStoreMeta).getCapacityBytesOnTiers();
  }

  @Test
  public void getUsedBytesOnTiersTest() throws Exception {
    Random random = new Random();
    WorkerStorageTierAssoc tierAssoc = new WorkerStorageTierAssoc(MasterContext.getConf());
    int nTiers = tierAssoc.size();
    // LinkedHashMap keeps keys in the serialized json object in the insertion order, the insertion
    // order is from smaller tier ordinal to larger ones.
    LinkedHashMap<String, Long> usedBytesOnTiers = new LinkedHashMap<>();
    for (int ordinal = 0; ordinal < nTiers; ordinal++) {
      usedBytesOnTiers.put(tierAssoc.getAlias(ordinal), random.nextLong());
    }
    Mockito.doReturn(usedBytesOnTiers).when(mStoreMeta).getUsedBytesOnTiers();

    new TestCase(mHostname, mPort,
        getEndpoint(AlluxioWorkerRestServiceHandler.GET_USED_BYTES_ON_TIERS), NO_PARAMS,
        HttpMethod.GET, usedBytesOnTiers).run();

    Mockito.verify(mStoreMeta).getUsedBytesOnTiers();
  }

  @Test
  public void getDirectoryPathsOnTiersTest() throws Exception {
    WorkerStorageTierAssoc tierAssoc = new WorkerStorageTierAssoc(MasterContext.getConf());
    int nTiers = tierAssoc.size();
    // LinkedHashMap keeps keys in the serialized json object in the insertion order, the insertion
    // order is from smaller tier ordinal to larger ones.
    LinkedHashMap<String, List<String>> pathsOnTiers = new LinkedHashMap<>();
    for (int ordinal = 0; ordinal < nTiers; ordinal++) {
      List<String> paths = new LinkedList<>();
      paths.add(IntegrationTestUtils.randomString());
      pathsOnTiers.put(tierAssoc.getAlias(ordinal), paths);
    }
    Mockito.doReturn(pathsOnTiers).when(mStoreMeta).getDirectoryPathsOnTiers();

    new TestCase(mHostname, mPort,
        getEndpoint(AlluxioWorkerRestServiceHandler.GET_DIRECTORY_PATHS_ON_TIERS), NO_PARAMS,
        HttpMethod.GET, pathsOnTiers).run();

    Mockito.verify(mStoreMeta).getDirectoryPathsOnTiers();
  }

  @Test
  public void getStartTimeMsTest() throws Exception {
    Random random = new Random();
    long startTime = random.nextLong();
    Mockito.doReturn(startTime).when(mWorker).getStartTimeMs();

    new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.GET_START_TIME_MS),
        NO_PARAMS, HttpMethod.GET, startTime).run();
  }

  @Test
  public void getUptimeMsTest() throws Exception {
    Random random = new Random();
    long uptime = random.nextLong();
    Mockito.doReturn(uptime).when(mWorker).getUptimeMs();

    new TestCase(mHostname, mPort, getEndpoint(AlluxioWorkerRestServiceHandler.GET_UPTIME_MS),
        NO_PARAMS, HttpMethod.GET, uptime).run();

    Mockito.verify(mWorker).getUptimeMs();
  }
}
