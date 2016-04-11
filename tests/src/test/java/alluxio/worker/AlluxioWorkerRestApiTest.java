/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker;

import alluxio.IntegrationTestUtils;
import alluxio.LocalAlluxioClusterResource;
import alluxio.Version;
import alluxio.WorkerStorageTierAssoc;
import alluxio.master.MasterContext;
import alluxio.metrics.MetricsSystem;
import alluxio.rest.TestCaseFactory;
import alluxio.util.CommonUtils;
import alluxio.worker.block.BlockStoreMeta;
import alluxio.worker.block.BlockWorker;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;

/**
 * Test cases for {@link AlluxioWorkerRestServiceHandler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioWorker.class, BlockWorker.class, BlockStoreMeta.class})
@Ignore
public final class AlluxioWorkerRestApiTest {
  private static final Map<String, String> NO_PARAMS = Maps.newHashMap();
  private static AlluxioWorker sWorker;
  private static BlockStoreMeta sStoreMeta;

  @Rule
  private LocalAlluxioClusterResource mResource = new LocalAlluxioClusterResource();

  @BeforeClass
  public static void beforeClass() {
    sWorker = PowerMockito.mock(AlluxioWorker.class);
    BlockWorker blockWorker = PowerMockito.mock(BlockWorker.class);
    sStoreMeta = PowerMockito.mock(BlockStoreMeta.class);
    Mockito.doReturn(sStoreMeta).when(blockWorker).getStoreMeta();
    Mockito.doReturn(blockWorker).when(sWorker).getBlockWorker();
    Whitebox.setInternalState(AlluxioWorker.class, "sAlluxioWorker", sWorker);
  }

  private String getEndpoint(String suffix) {
    return AlluxioWorkerRestServiceHandler.SERVICE_PREFIX + "/" + suffix;
  }

  @Test
  public void getRpcAddressTest() throws Exception {
    Random random = new Random();
    InetSocketAddress address = new InetSocketAddress(IntegrationTestUtils.randomString(),
        random.nextInt(8080) + 1);
    Mockito.doReturn(address).when(sWorker).getWorkerAddress();

    TestCaseFactory
        .newWorkerTestCase(getEndpoint(AlluxioWorkerRestServiceHandler.GET_RPC_ADDRESS), NO_PARAMS,
            "GET", address.toString(), mResource).run();

    Mockito.verify(sWorker).getWorkerAddress();
  }

  @Test
  public void getCapacityBytesTest() throws Exception {
    Random random = new Random();
    long capacityBytes = random.nextLong();
    Mockito.doReturn(capacityBytes).when(sStoreMeta).getCapacityBytes();

    TestCaseFactory
        .newWorkerTestCase(getEndpoint(AlluxioWorkerRestServiceHandler.GET_CAPACITY_BYTES),
            NO_PARAMS, "GET", capacityBytes, mResource).run();
  }

  @Test
  public void getUsedBytesTest() throws Exception {
    Random random = new Random();
    long usedBytes = random.nextLong();
    Mockito.doReturn(usedBytes).when(sStoreMeta).getUsedBytes();

    TestCaseFactory
        .newWorkerTestCase(getEndpoint(AlluxioWorkerRestServiceHandler.GET_USED_BYTES),
            NO_PARAMS, "GET", usedBytes, mResource).run();
  }

  @Test
  public void getMetricsTest() throws Exception {
    // Mock worker metrics system.
    MetricRegistry metricRegistry = PowerMockito.mock(MetricRegistry.class);
    MetricsSystem metricsSystem = PowerMockito.mock(MetricsSystem.class);
    Mockito.doReturn(metricRegistry).when(metricsSystem).getMetricRegistry();
    Mockito.doReturn(metricsSystem).when(sWorker).getWorkerMetricsSystem();

    // Generate random metrics.
    Random random = new Random();
    SortedMap<String, Long> metricsMap = Maps.newTreeMap();
    metricsMap.put(IntegrationTestUtils.randomString(), random.nextLong());
    metricsMap.put(IntegrationTestUtils.randomString(), random.nextLong());
    String blocksCachedProperty = CommonUtils.argsToString(".",
        WorkerContext.getWorkerSource().getName(), WorkerSource.BLOCKS_CACHED);
    Integer blocksCached = random.nextInt();
    metricsMap.put(blocksCachedProperty, blocksCached.longValue());

    // Mock counters.
    SortedMap<String, Counter> counters = Maps.newTreeMap();
    for (Map.Entry<String, Long> entry : metricsMap.entrySet()) {
      Counter counter = new Counter();
      counter.inc(entry.getValue());
      counters.put(entry.getKey(), counter);
    }
    Mockito.doReturn(counters).when(metricRegistry).getCounters();

    // Mock gauges.
    Gauge<?> blocksCachedGauge = PowerMockito.mock(Gauge.class);
    Mockito.doReturn(blocksCached).when(blocksCachedGauge).getValue();
    SortedMap<String, Gauge<?>> gauges = Maps.newTreeMap();
    gauges.put(blocksCachedProperty, blocksCachedGauge);
    Mockito.doReturn(gauges).when(metricRegistry).getGauges();

    TestCaseFactory
        .newWorkerTestCase(getEndpoint(AlluxioWorkerRestServiceHandler.GET_METRICS), NO_PARAMS,
            "GET", metricsMap, mResource).run();

    Mockito.verify(metricRegistry).getCounters();
    Mockito.verify(metricRegistry).getGauges();
    Mockito.verify(blocksCachedGauge).getValue();
  }

  @Test
  public void getVersionTest() throws Exception {
    TestCaseFactory.newWorkerTestCase(getEndpoint(AlluxioWorkerRestServiceHandler.GET_VERSION),
        NO_PARAMS, "GET", Version.VERSION, mResource).run();
  }

  @Test
  public void getCapacityBytesOnTiersTest() throws Exception {
    Random random = new Random();
    WorkerStorageTierAssoc tierAssoc = new WorkerStorageTierAssoc(MasterContext.getConf());
    int nTiers = tierAssoc.size();
    // LinkedHashMap keeps keys in the serialized json object in the insertion order, the insertion
    // order is from smaller tier ordinal to larger ones.
    LinkedHashMap<String, Long> capacityBytesOnTiers = Maps.newLinkedHashMap();
    for (int ordinal = 0; ordinal < nTiers; ordinal++) {
      capacityBytesOnTiers.put(tierAssoc.getAlias(ordinal), random.nextLong());
    }
    Mockito.doReturn(capacityBytesOnTiers).when(sStoreMeta).getCapacityBytesOnTiers();

    TestCaseFactory
        .newWorkerTestCase(getEndpoint(AlluxioWorkerRestServiceHandler.GET_CAPACITY_BYTES_ON_TIERS),
            NO_PARAMS, "GET", capacityBytesOnTiers, mResource).run();

    Mockito.verify(sStoreMeta).getCapacityBytesOnTiers();
  }

  @Test
  public void getUsedBytesOnTiersTest() throws Exception {
    Random random = new Random();
    WorkerStorageTierAssoc tierAssoc = new WorkerStorageTierAssoc(MasterContext.getConf());
    int nTiers = tierAssoc.size();
    // LinkedHashMap keeps keys in the serialized json object in the insertion order, the insertion
    // order is from smaller tier ordinal to larger ones.
    LinkedHashMap<String, Long> usedBytesOnTiers = Maps.newLinkedHashMap();
    for (int ordinal = 0; ordinal < nTiers; ordinal++) {
      usedBytesOnTiers.put(tierAssoc.getAlias(ordinal), random.nextLong());
    }
    Mockito.doReturn(usedBytesOnTiers).when(sStoreMeta).getUsedBytesOnTiers();

    TestCaseFactory
        .newWorkerTestCase(getEndpoint(AlluxioWorkerRestServiceHandler.GET_USED_BYTES_ON_TIERS),
            NO_PARAMS, "GET", usedBytesOnTiers, mResource).run();

    Mockito.verify(sStoreMeta).getUsedBytesOnTiers();
  }

  @Test
  public void getDirectoryPathsOnTiersTest() throws Exception {
    WorkerStorageTierAssoc tierAssoc = new WorkerStorageTierAssoc(MasterContext.getConf());
    int nTiers = tierAssoc.size();
    // LinkedHashMap keeps keys in the serialized json object in the insertion order, the insertion
    // order is from smaller tier ordinal to larger ones.
    LinkedHashMap<String, List<String>> pathsOnTiers = Maps.newLinkedHashMap();
    for (int ordinal = 0; ordinal < nTiers; ordinal++) {
      List<String> paths = Lists.newLinkedList();
      paths.add(IntegrationTestUtils.randomString());
      pathsOnTiers.put(tierAssoc.getAlias(ordinal), paths);
    }
    Mockito.doReturn(pathsOnTiers).when(sStoreMeta).getDirectoryPathsOnTiers();

    TestCaseFactory.newWorkerTestCase(getEndpoint(
        AlluxioWorkerRestServiceHandler.GET_DIRECTORY_PATHS_ON_TIERS), NO_PARAMS, "GET",
        pathsOnTiers, mResource).run();

    Mockito.verify(sStoreMeta).getDirectoryPathsOnTiers();
  }

  @Test
  public void getStartTimeMsTest() throws Exception {
    Random random = new Random();
    long startTime = random.nextLong();
    Mockito.doReturn(startTime).when(sWorker).getStartTimeMs();

    TestCaseFactory
        .newWorkerTestCase(getEndpoint(AlluxioWorkerRestServiceHandler.GET_START_TIME_MS),
            NO_PARAMS, "GET", startTime, mResource).run();
  }

  @Test
  public void getUptimeMsTest() throws Exception {
    Random random = new Random();
    long uptime = random.nextLong();
    Mockito.doReturn(uptime).when(sWorker).getUptimeMs();

    TestCaseFactory
        .newWorkerTestCase(getEndpoint(AlluxioWorkerRestServiceHandler.GET_UPTIME_MS), NO_PARAMS,
            "GET", uptime, mResource).run();

    Mockito.verify(sWorker).getUptimeMs();
  }
}
