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

package alluxio.master;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.IntegrationTestUtils;
import alluxio.LocalAlluxioClusterResource;
import alluxio.MasterStorageTierAssoc;
import alluxio.Version;
import alluxio.WorkerStorageTierAssoc;
import alluxio.master.block.BlockMaster;
import alluxio.metrics.MetricsSystem;
import alluxio.rest.TestCaseFactory;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerInfoTest;

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
import java.util.Properties;
import java.util.Random;
import java.util.SortedMap;

/**
 * Test cases for {@link AlluxioMasterRestServiceHandler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioMaster.class, BlockMaster.class, Configuration.class, MasterContext.class,
    MetricRegistry.class, UnderFileSystem.class})
@Ignore
public final class AlluxioMasterRestApiTest {
  private static final String ALLUXIO_CONF_PREFIX = "alluxio";
  private static final String NOT_ALLUXIO_CONF_PREFIX = "_alluxio_";
  private static final Map<String, String> NO_PARAMS = Maps.newHashMap();
  private static AlluxioMaster sAlluxioMaster;
  private static BlockMaster sBlockMaster;

  @Rule
  private LocalAlluxioClusterResource mResource = new LocalAlluxioClusterResource();

  @BeforeClass
  public static void beforeClass() {
    sAlluxioMaster = PowerMockito.mock(AlluxioMaster.class);
    sBlockMaster = PowerMockito.mock(BlockMaster.class);
    Mockito.doReturn(sBlockMaster).when(sAlluxioMaster).getBlockMaster();
    Whitebox.setInternalState(AlluxioMaster.class, "sAlluxioMaster", sAlluxioMaster);
  }

  private String getEndpoint(String suffix) {
    return AlluxioMasterRestServiceHandler.SERVICE_PREFIX + "/" + suffix;
  }

  @Test
  public void getCapacityBytesTest() throws Exception {
    Random random = new Random();
    long capacityBytes = random.nextLong();
    Mockito.doReturn(capacityBytes).when(sBlockMaster).getCapacityBytes();

    TestCaseFactory
        .newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_CAPACITY_BYTES),
            NO_PARAMS, "GET", capacityBytes, mResource).run();
  }

  @Test
  public void getUsedBytesTest() throws Exception {
    Random random = new Random();
    long usedBytes = random.nextLong();
    Mockito.doReturn(usedBytes).when(sBlockMaster).getUsedBytes();

    TestCaseFactory
        .newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_USED_BYTES),
            NO_PARAMS, "GET", usedBytes, mResource).run();
  }

  @Test
  public void getFreeBytesTest() throws Exception {
    Random random = new Random();
    long capacityBytes = random.nextLong();
    long usedBytes = random.nextLong();
    Mockito.doReturn(capacityBytes).when(sBlockMaster).getCapacityBytes();
    Mockito.doReturn(usedBytes).when(sBlockMaster).getUsedBytes();

    TestCaseFactory
        .newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_FREE_BYTES), NO_PARAMS,
            "GET", capacityBytes - usedBytes, mResource).run();
  }

  @Test
  public void getWorkerCountTest() throws Exception {
    Random random = new Random();
    int workerCount = random.nextInt();
    Mockito.doReturn(workerCount).when(sBlockMaster).getWorkerCount();

    TestCaseFactory
        .newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_WORKER_COUNT), NO_PARAMS,
            "GET", workerCount, mResource).run();

    Mockito.verify(sBlockMaster).getWorkerCount();
  }

  @Test
  public void getWorkerInfoListTest() throws Exception {
    Random random = new Random();
    List<WorkerInfo> workerInfos = Lists.newArrayList();
    int numWorkerInfos = random.nextInt(10);
    for (int i = 0; i < numWorkerInfos; i++) {
      workerInfos.add(WorkerInfoTest.createRandom());
    }
    Mockito.doReturn(workerInfos).when(sBlockMaster).getWorkerInfoList();

    TestCaseFactory
        .newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_WORKER_INFO_LIST),
            NO_PARAMS, "GET", workerInfos, mResource).run();

    Mockito.verify(sBlockMaster).getWorkerInfoList();
  }

  private Configuration mockConfiguration() {
    Configuration conf = PowerMockito.spy(MasterContext.getConf());
    PowerMockito.spy(MasterContext.class);
    Mockito.when(MasterContext.getConf()).thenReturn(conf);
    return conf;
  }

  @Test
  public void getConfigurationTest() throws Exception {
    SortedMap<String, String> propertyMap = Maps.newTreeMap();
    propertyMap.put(ALLUXIO_CONF_PREFIX + IntegrationTestUtils.randomString(),
        IntegrationTestUtils.randomString());
    propertyMap.put(ALLUXIO_CONF_PREFIX + IntegrationTestUtils.randomString(),
        IntegrationTestUtils.randomString());

    Properties properties = new Properties();
    for (Map.Entry<String, String> property : propertyMap.entrySet()) {
      properties.put(property.getKey(), property.getValue());
    }
    properties.put(NOT_ALLUXIO_CONF_PREFIX + IntegrationTestUtils.randomString(),
        IntegrationTestUtils.randomString());

    Configuration configuration = mockConfiguration();
    Mockito.doReturn(properties).when(configuration).getInternalProperties();

    TestCaseFactory
        .newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_CONFIGURATION),
            NO_PARAMS, "GET", propertyMap, mResource).run();

    Mockito.verify(configuration).getInternalProperties();
  }

  @Test
  public void getRpcAddressTest() throws Exception {
    Random random = new Random();
    InetSocketAddress address = new InetSocketAddress(IntegrationTestUtils.randomString(),
        random.nextInt(8080) + 1);
    Mockito.doReturn(address).when(sAlluxioMaster).getMasterAddress();

    TestCaseFactory
        .newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_RPC_ADDRESS), NO_PARAMS,
            "GET", address.toString(), mResource).run();

    Mockito.verify(sAlluxioMaster).getMasterAddress();
  }

  @Test
  public void getMetricsTest() throws Exception {
    // Mock master metrics system.
    MetricRegistry metricRegistry = PowerMockito.mock(MetricRegistry.class);
    MetricsSystem metricsSystem = PowerMockito.mock(MetricsSystem.class);
    Mockito.doReturn(metricRegistry).when(metricsSystem).getMetricRegistry();
    Mockito.doReturn(metricsSystem).when(sAlluxioMaster).getMasterMetricsSystem();

    // Generate random metrics.
    Random random = new Random();
    SortedMap<String, Long> metricsMap = Maps.newTreeMap();
    metricsMap.put(IntegrationTestUtils.randomString(), random.nextLong());
    metricsMap.put(IntegrationTestUtils.randomString(), random.nextLong());
    String filesPinnedProperty = CommonUtils.argsToString(".",
        MasterContext.getMasterSource().getName(), MasterSource.FILES_PINNED);
    Integer filesPinned = random.nextInt();
    metricsMap.put(filesPinnedProperty, filesPinned.longValue());

    // Mock counters.
    SortedMap<String, Counter> counters = Maps.newTreeMap();
    for (Map.Entry<String, Long> entry : metricsMap.entrySet()) {
      Counter counter = new Counter();
      counter.inc(entry.getValue());
      counters.put(entry.getKey(), counter);
    }
    Mockito.doReturn(counters).when(metricRegistry).getCounters();

    // Mock gauges.
    Gauge<?> filesPinnedGauge = PowerMockito.mock(Gauge.class);
    Mockito.doReturn(filesPinned).when(filesPinnedGauge).getValue();
    SortedMap<String, Gauge<?>> gauges = Maps.newTreeMap();
    gauges.put(filesPinnedProperty, filesPinnedGauge);
    Mockito.doReturn(gauges).when(metricRegistry).getGauges();

    TestCaseFactory
        .newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_METRICS), NO_PARAMS,
            "GET", metricsMap, mResource).run();

    Mockito.verify(metricRegistry).getCounters();
    Mockito.verify(metricRegistry).getGauges();
    Mockito.verify(filesPinnedGauge).getValue();
  }

  @Test
  public void getStartTimeMsTest() throws Exception {
    Random random = new Random();
    long startTime = random.nextLong();
    Mockito.doReturn(startTime).when(sAlluxioMaster).getStartTimeMs();

    TestCaseFactory
        .newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_START_TIME_MS),
            NO_PARAMS, "GET", startTime, mResource).run();
  }

  @Test
  public void getUptimeMsTest() throws Exception {
    Random random = new Random();
    long uptime = random.nextLong();
    Mockito.doReturn(uptime).when(sAlluxioMaster).getUptimeMs();

    TestCaseFactory
        .newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_UPTIME_MS), NO_PARAMS,
            "GET", uptime, mResource).run();

    Mockito.verify(sAlluxioMaster).getUptimeMs();
  }

  @Test
  public void getVersionTest() throws Exception {
    TestCaseFactory.newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_VERSION),
        NO_PARAMS, "GET", Version.VERSION, mResource).run();
  }

  private UnderFileSystem mockUfs() {
    Configuration masterConf = MasterContext.getConf();
    UnderFileSystem ufs = PowerMockito.spy(UnderFileSystem.get(masterConf.get(
        Constants.UNDERFS_ADDRESS), masterConf));
    PowerMockito.mockStatic(UnderFileSystem.class);
    Mockito.when(UnderFileSystem.get(Mockito.anyString(), Mockito.any(Configuration.class)))
        .thenReturn(ufs);
    return ufs;
  }

  @Test
  public void getUfsCapacityBytesTest() throws Exception {
    UnderFileSystem ufs = mockUfs();

    Random random = new Random();
    long capacity = random.nextLong();
    Mockito.doReturn(capacity).when(ufs).getSpace(Mockito.anyString(), Mockito.eq(
        UnderFileSystem.SpaceType.SPACE_TOTAL));

    TestCaseFactory
        .newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_UFS_CAPACITY_BYTES),
            NO_PARAMS, "GET", capacity, mResource).run();
  }

  @Test
  public void getUfsUsedBytesTest() throws Exception {
    UnderFileSystem ufs = mockUfs();

    Random random = new Random();
    long usedBytes = random.nextLong();
    Mockito.doReturn(usedBytes).when(ufs).getSpace(Mockito.anyString(), Mockito.eq(
        UnderFileSystem.SpaceType.SPACE_USED));

    TestCaseFactory
        .newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_UFS_USED_BYTES),
            NO_PARAMS, "GET", usedBytes, mResource).run();
  }

  @Test
  public void getUfsFreeBytesTest() throws Exception {
    UnderFileSystem ufs = mockUfs();

    Random random = new Random();
    long freeBytes = random.nextLong();
    Mockito.doReturn(freeBytes).when(ufs).getSpace(Mockito.anyString(), Mockito.eq(
        UnderFileSystem.SpaceType.SPACE_FREE));

    TestCaseFactory
        .newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_UFS_FREE_BYTES),
            NO_PARAMS, "GET", freeBytes, mResource).run();
  }

  @Test
  public void getCapacityBytesOnTiersTest() throws Exception {
    Random random = new Random();
    MasterStorageTierAssoc tierAssoc = new MasterStorageTierAssoc(MasterContext.getConf());
    int nTiers = tierAssoc.size();
    // LinkedHashMap keeps keys in the serialized json object in the insertion order, the insertion
    // order is from smaller tier ordinal to larger ones.
    LinkedHashMap<String, Long> capacityBytesOnTiers = Maps.newLinkedHashMap();
    for (int ordinal = 0; ordinal < nTiers; ordinal++) {
      capacityBytesOnTiers.put(tierAssoc.getAlias(ordinal), random.nextLong());
    }
    Mockito.doReturn(capacityBytesOnTiers).when(sBlockMaster).getTotalBytesOnTiers();

    TestCaseFactory
        .newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_CAPACITY_BYTES_ON_TIERS),
            NO_PARAMS, "GET", capacityBytesOnTiers, mResource).run();

    Mockito.verify(sBlockMaster).getTotalBytesOnTiers();
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
    Mockito.doReturn(usedBytesOnTiers).when(sBlockMaster).getUsedBytesOnTiers();

    TestCaseFactory
        .newMasterTestCase(getEndpoint(AlluxioMasterRestServiceHandler.GET_USED_BYTES_ON_TIERS),
            NO_PARAMS, "GET", usedBytesOnTiers, mResource).run();

    Mockito.verify(sBlockMaster).getUsedBytesOnTiers();
  }
}
