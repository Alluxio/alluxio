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
import alluxio.MasterStorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.cli.Version;
import alluxio.master.block.BlockMaster;
import alluxio.metrics.MetricsSystem;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerInfoTest;

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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link AlluxioMasterRestServiceHandler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AlluxioMaster.class, BlockMaster.class, Configuration.class, MasterContext.class,
    MetricRegistry.class, UnderFileSystem.class})
public final class AlluxioMasterRestApiTest extends RestApiTest {
  private static final String ALLUXIO_CONF_PREFIX = "alluxio";
  private static final String NOT_ALLUXIO_CONF_PREFIX = "_alluxio_";
  private AlluxioMaster mAlluxioMaster;
  private BlockMaster mBlockMaster;

  @Before
  public void before() {
    mAlluxioMaster = PowerMockito.spy(mResource.get().getMaster().getInternalMaster());
    Whitebox.setInternalState(AlluxioMaster.class, "sAlluxioMaster", mAlluxioMaster);
    mBlockMaster = PowerMockito.mock(BlockMaster.class);
    Whitebox.setInternalState(mAlluxioMaster, "mBlockMaster", mBlockMaster);
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getMaster().getWebLocalPort();
    mServicePrefix = AlluxioMasterRestServiceHandler.SERVICE_PREFIX;
  }

  @Test
  public void getCapacityBytesTest() throws Exception {
    Random random = new Random();
    long capacityBytes = random.nextLong();
    Mockito.doReturn(capacityBytes).when(mBlockMaster).getCapacityBytes();

    new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_CAPACITY_BYTES),
        NO_PARAMS, HttpMethod.GET, capacityBytes).run();
  }

  @Test
  public void getUsedBytesTest() throws Exception {
    Random random = new Random();
    long usedBytes = random.nextLong();
    Mockito.doReturn(usedBytes).when(mBlockMaster).getUsedBytes();

    new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_USED_BYTES),
            NO_PARAMS, HttpMethod.GET, usedBytes).run();
  }

  @Test
  public void getFreeBytesTest() throws Exception {
    Random random = new Random();
    long capacityBytes = random.nextLong();
    long usedBytes = random.nextLong();
    Mockito.doReturn(capacityBytes).when(mBlockMaster).getCapacityBytes();
    Mockito.doReturn(usedBytes).when(mBlockMaster).getUsedBytes();

    new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_FREE_BYTES),
        NO_PARAMS, HttpMethod.GET, capacityBytes - usedBytes).run();
  }

  @Test
  public void getWorkerCountTest() throws Exception {
    Random random = new Random();
    int workerCount = random.nextInt();
    Mockito.doReturn(workerCount).when(mBlockMaster).getWorkerCount();

    new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_WORKER_COUNT),
        NO_PARAMS, HttpMethod.GET, workerCount).run();

    Mockito.verify(mBlockMaster).getWorkerCount();
  }

  @Test
  public void getWorkerInfoListTest() throws Exception {
    Random random = new Random();
    List<WorkerInfo> workerInfos = new ArrayList<>();
    int numWorkerInfos = random.nextInt(10);
    for (int i = 0; i < numWorkerInfos; i++) {
      workerInfos.add(WorkerInfoTest.createRandom());
    }
    Mockito.doReturn(workerInfos).when(mBlockMaster).getWorkerInfoList();

    new TestCase(mHostname, mPort,
        getEndpoint(AlluxioMasterRestServiceHandler.GET_WORKER_INFO_LIST), NO_PARAMS,
        HttpMethod.GET, workerInfos).run();

    Mockito.verify(mBlockMaster).getWorkerInfoList();
  }

  private Configuration mockConfiguration() {
    Configuration conf = PowerMockito.spy(MasterContext.getConf());
    PowerMockito.spy(MasterContext.class);
    Mockito.when(MasterContext.getConf()).thenReturn(conf);
    return conf;
  }

  @Test
  public void getConfigurationTest() throws Exception {
    SortedMap<String, String> propertyMap = new TreeMap<>();
    propertyMap.put(ALLUXIO_CONF_PREFIX + CommonUtils.randomString(10),
        CommonUtils.randomString(10));
    propertyMap.put(ALLUXIO_CONF_PREFIX + CommonUtils.randomString(10),
        CommonUtils.randomString(10));

    Properties properties = new Properties();
    for (Map.Entry<String, String> property : propertyMap.entrySet()) {
      properties.put(property.getKey(), property.getValue());
    }
    properties.put(NOT_ALLUXIO_CONF_PREFIX + CommonUtils.randomString(10),
        CommonUtils.randomString(10));

    Configuration configuration = mockConfiguration();
    Mockito.doReturn(properties).when(configuration).getInternalProperties();

    new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_CONFIGURATION),
        NO_PARAMS, HttpMethod.GET, propertyMap).run();

    Mockito.verify(configuration).getInternalProperties();
  }

  @Test
  public void getRpcAddressTest() throws Exception {
    Random random = new Random();
    InetSocketAddress address = new InetSocketAddress(CommonUtils.randomString(10),
        random.nextInt(8080) + 1);
    Mockito.doReturn(address).when(mAlluxioMaster).getMasterAddress();

    new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_RPC_ADDRESS),
        NO_PARAMS, HttpMethod.GET, address.toString()).run();

    Mockito.verify(mAlluxioMaster).getMasterAddress();
  }

  @Test
  public void getMetricsTest() throws Exception {
    // Mock master metrics system.
    MetricRegistry metricRegistry = PowerMockito.mock(MetricRegistry.class);
    MetricsSystem metricsSystem = PowerMockito.mock(MetricsSystem.class);
    Mockito.doReturn(metricRegistry).when(metricsSystem).getMetricRegistry();
    Mockito.doReturn(metricsSystem).when(mAlluxioMaster).getMasterMetricsSystem();

    // Generate random metrics.
    Random random = new Random();
    SortedMap<String, Long> metricsMap = new TreeMap<>();
    metricsMap.put(CommonUtils.randomString(10), random.nextLong());
    metricsMap.put(CommonUtils.randomString(10), random.nextLong());
    String filesPinnedProperty = CommonUtils.argsToString(".",
        MasterContext.getMasterSource().getName(), MasterSource.FILES_PINNED);
    Integer filesPinned = random.nextInt();
    metricsMap.put(filesPinnedProperty, filesPinned.longValue());

    // Mock counters.
    SortedMap<String, Counter> counters = new TreeMap<>();
    for (Map.Entry<String, Long> entry : metricsMap.entrySet()) {
      Counter counter = new Counter();
      counter.inc(entry.getValue());
      counters.put(entry.getKey(), counter);
    }
    Mockito.doReturn(counters).when(metricRegistry).getCounters();

    // Mock gauges.
    Gauge filesPinnedGauge = PowerMockito.mock(Gauge.class);
    Mockito.doReturn(filesPinned).when(filesPinnedGauge).getValue();
    SortedMap<String, Gauge<?>> gauges = new TreeMap<>();
    gauges.put(filesPinnedProperty, filesPinnedGauge);
    Mockito.doReturn(gauges).when(metricRegistry).getGauges();

    new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_METRICS),
        NO_PARAMS, HttpMethod.GET, metricsMap).run();

    Mockito.verify(metricRegistry).getCounters();
    Mockito.verify(metricRegistry).getGauges();
    Mockito.verify(filesPinnedGauge).getValue();
  }

  @Test
  public void getStartTimeMsTest() throws Exception {
    Random random = new Random();
    long startTime = random.nextLong();
    Mockito.doReturn(startTime).when(mAlluxioMaster).getStartTimeMs();

    new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_START_TIME_MS),
        NO_PARAMS, HttpMethod.GET, startTime).run();
  }

  @Test
  public void getUptimeMsTest() throws Exception {
    Random random = new Random();
    long uptime = random.nextLong();
    Mockito.doReturn(uptime).when(mAlluxioMaster).getUptimeMs();

    new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_UPTIME_MS),
        NO_PARAMS, HttpMethod.GET, uptime).run();

    Mockito.verify(mAlluxioMaster).getUptimeMs();
  }

  @Test
  public void getVersionTest() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_VERSION),
        NO_PARAMS, HttpMethod.GET, Version.VERSION).run();
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

    new TestCase(mHostname, mPort,
        getEndpoint(AlluxioMasterRestServiceHandler.GET_UFS_CAPACITY_BYTES), NO_PARAMS,
        HttpMethod.GET, capacity).run();
  }

  @Test
  public void getUfsUsedBytesTest() throws Exception {
    UnderFileSystem ufs = mockUfs();

    Random random = new Random();
    long usedBytes = random.nextLong();
    Mockito.doReturn(usedBytes).when(ufs).getSpace(Mockito.anyString(), Mockito.eq(
        UnderFileSystem.SpaceType.SPACE_USED));

    new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_UFS_USED_BYTES),
        NO_PARAMS, HttpMethod.GET, usedBytes).run();
  }

  @Test
  public void getUfsFreeBytesTest() throws Exception {
    UnderFileSystem ufs = mockUfs();

    Random random = new Random();
    long freeBytes = random.nextLong();
    Mockito.doReturn(freeBytes).when(ufs).getSpace(Mockito.anyString(), Mockito.eq(
        UnderFileSystem.SpaceType.SPACE_FREE));

    new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_UFS_FREE_BYTES),
        NO_PARAMS, HttpMethod.GET, freeBytes).run();
  }

  @Test
  public void getCapacityBytesOnTiersTest() throws Exception {
    Random random = new Random();
    MasterStorageTierAssoc tierAssoc = new MasterStorageTierAssoc(MasterContext.getConf());
    int nTiers = tierAssoc.size();
    // LinkedHashMap keeps keys in the serialized json object in the insertion order, the insertion
    // order is from smaller tier ordinal to larger ones.
    LinkedHashMap<String, Long> capacityBytesOnTiers = new LinkedHashMap<>();
    for (int ordinal = 0; ordinal < nTiers; ordinal++) {
      capacityBytesOnTiers.put(tierAssoc.getAlias(ordinal), random.nextLong());
    }
    Mockito.doReturn(capacityBytesOnTiers).when(mBlockMaster).getTotalBytesOnTiers();

    new TestCase(mHostname, mPort,
        getEndpoint(AlluxioMasterRestServiceHandler.GET_CAPACITY_BYTES_ON_TIERS), NO_PARAMS,
        HttpMethod.GET, capacityBytesOnTiers).run();

    Mockito.verify(mBlockMaster).getTotalBytesOnTiers();
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
    Mockito.doReturn(usedBytesOnTiers).when(mBlockMaster).getUsedBytesOnTiers();

    new TestCase(mHostname, mPort,
        getEndpoint(AlluxioMasterRestServiceHandler.GET_USED_BYTES_ON_TIERS), NO_PARAMS,
        HttpMethod.GET, usedBytesOnTiers).run();

    Mockito.verify(mBlockMaster).getUsedBytesOnTiers();
  }
}
