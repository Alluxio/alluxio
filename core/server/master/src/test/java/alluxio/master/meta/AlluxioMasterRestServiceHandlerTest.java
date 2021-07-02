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

package alluxio.master.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.RuntimeConstants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.StorageList;
import alluxio.master.AlluxioMasterProcess;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.FileSystemMasterFactory;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.meta.Block;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.web.MasterWebServer;
import alluxio.wire.AlluxioMasterInfo;
import alluxio.wire.Capacity;
import alluxio.wire.MountPointInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Matchers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.ws.rs.core.Response;

/**
 * Unit tests for {@link AlluxioMasterRestServiceHandler}.
 */
public final class AlluxioMasterRestServiceHandlerTest {
  private static final WorkerNetAddress NET_ADDRESS_1 = new WorkerNetAddress().setHost("localhost")
      .setRpcPort(80).setDataPort(81).setWebPort(82);
  private static final WorkerNetAddress NET_ADDRESS_2 = new WorkerNetAddress().setHost("localhost")
      .setRpcPort(83).setDataPort(84).setWebPort(85);
  private static final Map<Block.BlockLocation, List<Long>> NO_BLOCKS_ON_LOCATIONS
      = ImmutableMap.of();
  private static final Map<String, StorageList> NO_LOST_STORAGE = ImmutableMap.of();

  private static final long UFS_SPACE_TOTAL = 100L;
  private static final long UFS_SPACE_USED = 25L;
  private static final long UFS_SPACE_FREE = 75L;
  private static final String TEST_PATH = "test://test/";
  private static final Map<String, Long> WORKER1_TOTAL_BYTES_ON_TIERS =
      ImmutableMap.of(Constants.MEDIUM_MEM, 10L,
      Constants.MEDIUM_SSD, 20L);
  private static final Map<String, Long> WORKER2_TOTAL_BYTES_ON_TIERS =
      ImmutableMap.of(Constants.MEDIUM_MEM,
      1000L, Constants.MEDIUM_SSD, 2000L);
  private static final Map<String, Long> WORKER1_USED_BYTES_ON_TIERS =
      ImmutableMap.of(Constants.MEDIUM_MEM, 1L,
      Constants.MEDIUM_SSD, 2L);
  private static final Map<String, Long> WORKER2_USED_BYTES_ON_TIERS =
      ImmutableMap.of(Constants.MEDIUM_MEM, 100L,
      Constants.MEDIUM_SSD, 200L);

  private AlluxioMasterProcess mMasterProcess;
  private BlockMaster mBlockMaster;
  private FileSystemMaster mFileSystemMaster;
  private MasterRegistry mRegistry;
  private AlluxioMasterRestServiceHandler mHandler;
  private MetricsMaster mMetricsMaster;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Rule
  public ConfigurationRule mConfigurationRule = new ConfigurationRule(new HashMap() {
    {
      put(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, TEST_PATH);
    }
  }, ServerConfiguration.global());

  @Before
  public void before() throws Exception {
    mMasterProcess = mock(AlluxioMasterProcess.class);
    ServletContext context = mock(ServletContext.class);
    mRegistry = new MasterRegistry();
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();
    mMetricsMaster = new MetricsMasterFactory().create(mRegistry, masterContext);
    mRegistry.add(MetricsMaster.class, mMetricsMaster);
    registerMockUfs();
    mBlockMaster = new BlockMasterFactory().create(mRegistry, masterContext);
    mFileSystemMaster = new FileSystemMasterFactory().create(mRegistry, masterContext);
    mRegistry.start(true);
    when(mMasterProcess.getMaster(BlockMaster.class)).thenReturn(mBlockMaster);
    when(mMasterProcess.getMaster(FileSystemMaster.class)).thenReturn(mFileSystemMaster);
    when(context.getAttribute(MasterWebServer.ALLUXIO_MASTER_SERVLET_RESOURCE_KEY)).thenReturn(
        mMasterProcess);
    mHandler = new AlluxioMasterRestServiceHandler(context);
    // Register two workers
    long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    long worker2 = mBlockMaster.getWorkerId(NET_ADDRESS_2);
    List<String> tiers = Arrays.asList(Constants.MEDIUM_MEM, Constants.MEDIUM_SSD);

    mBlockMaster.workerRegister(worker1, tiers, WORKER1_TOTAL_BYTES_ON_TIERS,
        WORKER1_USED_BYTES_ON_TIERS, NO_BLOCKS_ON_LOCATIONS, NO_LOST_STORAGE,
        RegisterWorkerPOptions.getDefaultInstance());
    mBlockMaster.workerRegister(worker2, tiers, WORKER2_TOTAL_BYTES_ON_TIERS,
        WORKER2_USED_BYTES_ON_TIERS, NO_BLOCKS_ON_LOCATIONS, NO_LOST_STORAGE,
        RegisterWorkerPOptions.getDefaultInstance());

    String filesPinnedProperty = MetricKey.MASTER_FILES_PINNED.getName();
    MetricsSystem.METRIC_REGISTRY.remove(filesPinnedProperty);
  }

  private void registerMockUfs() throws IOException {
    UnderFileSystemFactory underFileSystemFactoryMock = mock(UnderFileSystemFactory.class);
    when(underFileSystemFactoryMock.supportsPath(anyString(), anyObject()))
        .thenReturn(Boolean.FALSE);
    when(underFileSystemFactoryMock.supportsPath(eq(TEST_PATH), anyObject()))
        .thenReturn(Boolean.TRUE);
    UnderFileSystem underFileSystemMock = mock(UnderFileSystem.class);
    when(underFileSystemMock.getSpace(TEST_PATH, UnderFileSystem.SpaceType.SPACE_FREE)).thenReturn(
        UFS_SPACE_FREE);
    when(underFileSystemMock.getSpace(TEST_PATH, UnderFileSystem.SpaceType.SPACE_TOTAL))
        .thenReturn(UFS_SPACE_TOTAL);
    when(underFileSystemMock.getSpace(TEST_PATH, UnderFileSystem.SpaceType.SPACE_USED)).thenReturn(
        UFS_SPACE_USED);
    when(underFileSystemFactoryMock.create(eq(TEST_PATH), Matchers.any()))
        .thenReturn(underFileSystemMock);
    UnderFileSystemFactoryRegistry.register(underFileSystemFactoryMock);
  }

  @After
  public void after() throws Exception {
    mRegistry.stop();
  }

  @Test
  public void getMasterInfo() {
    // Mock for rpc address
    when(mMasterProcess.getRpcAddress()).thenReturn(new InetSocketAddress("localhost", 8080));
    // Mock for metrics
    final int FILES_PINNED_TEST_VALUE = 100;
    String filesPinnedProperty = MetricKey.MASTER_FILES_PINNED.getName();
    Gauge<Integer> filesPinnedGauge = () -> FILES_PINNED_TEST_VALUE;
    MetricSet mockMetricsSet = mock(MetricSet.class);
    Map<String, Metric> map = new HashMap<>();
    map.put(filesPinnedProperty, filesPinnedGauge);
    when(mockMetricsSet.getMetrics()).thenReturn(map);
    MetricsSystem.METRIC_REGISTRY.registerAll(mockMetricsSet);
    // Mock for start time
    when(mMasterProcess.getStartTimeMs()).thenReturn(101L);
    // Mock for up time
    when(mMasterProcess.getUptimeMs()).thenReturn(102L);

    Response response = mHandler.getInfo(false);
    try {
      assertNotNull("Response must be not null!", response);
      assertNotNull("Response must have a entry!", response.getEntity());
      assertTrue("Entry must be an AlluxioMasterInfo!",
              (response.getEntity() instanceof AlluxioMasterInfo));
      AlluxioMasterInfo info = (AlluxioMasterInfo) response.getEntity();

      // Validate configuration
      assertNotNull("Configuration must be not null", info.getConfiguration());
      assertFalse("Properties Map must be not empty!", (info.getConfiguration().isEmpty()));
      // Validate rpc address
      assertEquals("localhost/127.0.0.1:8080", info.getRpcAddress());
      // Validate metrics
      Map<String, Long> metricsMap = info.getMetrics();
      assertFalse("Metrics Map must be not empty!", (metricsMap.isEmpty()));
      assertTrue("Map must contain key " + filesPinnedProperty + "!",
          metricsMap.containsKey(filesPinnedProperty));
      assertEquals(FILES_PINNED_TEST_VALUE, metricsMap.get(filesPinnedProperty).longValue());
      // Validate StartTimeMs
      assertEquals(101L, info.getStartTimeMs());
      // Validate UptimeMs
      assertEquals(102L, info.getUptimeMs());
      // Validate version
      assertEquals(RuntimeConstants.VERSION, info.getVersion());
      // Validate capacity bytes
      Capacity cap = info.getCapacity();
      long sumCapacityBytes = 0;
      for (Map.Entry<String, Long> entry1 : WORKER1_TOTAL_BYTES_ON_TIERS.entrySet()) {
        Long totalBytes = entry1.getValue();
        sumCapacityBytes += totalBytes;
      }
      for (Map.Entry<String, Long> entry1 : WORKER2_TOTAL_BYTES_ON_TIERS.entrySet()) {
        Long totalBytes = entry1.getValue();
        sumCapacityBytes += totalBytes;
      }
      assertEquals(sumCapacityBytes, cap.getTotal());
      // Validate used bytes
      long sumUsedBytes = 0;
      for (Map.Entry<String, Long> entry1 : WORKER1_USED_BYTES_ON_TIERS.entrySet()) {
        Long totalBytes = entry1.getValue();
        sumUsedBytes += totalBytes;
      }
      for (Map.Entry<String, Long> entry1 : WORKER2_USED_BYTES_ON_TIERS.entrySet()) {
        Long totalBytes = entry1.getValue();
        sumUsedBytes += totalBytes;
      }
      assertEquals(sumUsedBytes, cap.getUsed());
      // Validate UFS capacity
      Capacity ufsCapacity = info.getUfsCapacity();
      assertEquals(UFS_SPACE_TOTAL, ufsCapacity.getTotal());
      assertEquals(UFS_SPACE_USED, ufsCapacity.getUsed());
      // Validate workers
      List<WorkerInfo> workers = info.getWorkers();
      assertEquals(2, workers.size());
      long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);
      long worker2 = mBlockMaster.getWorkerId(NET_ADDRESS_2);
      Set<Long> expectedWorkers = new HashSet<>();
      expectedWorkers.add(worker1);
      expectedWorkers.add(worker2);
      Set<Long> actualWorkers = new HashSet<>();
      for (WorkerInfo w : workers) {
        actualWorkers.add(w.getId());
      }
      assertEquals(expectedWorkers, actualWorkers);
    } finally {
      response.close();
    }
  }

  @Test
  public void isMounted() {
    String s3Uri = "s3a://test/dir_1/dir-2";
    String hdfsUri = "hdfs://test";

    Map<String, MountPointInfo> mountTable = new HashMap<>();
    mountTable.put("/s3", new MountPointInfo().setUfsUri(s3Uri));
    FileSystemMaster mockMaster = mock(FileSystemMaster.class);
    when(mockMaster.getMountPointInfoSummary()).thenReturn(mountTable);

    AlluxioMasterProcess masterProcess = mock(AlluxioMasterProcess.class);
    when(masterProcess.getMaster(FileSystemMaster.class)).thenReturn(mockMaster);

    ServletContext context = mock(ServletContext.class);
    when(context.getAttribute(MasterWebServer.ALLUXIO_MASTER_SERVLET_RESOURCE_KEY)).thenReturn(
        masterProcess);
    AlluxioMasterRestServiceHandler handler = new AlluxioMasterRestServiceHandler(context);

    assertFalse(handler.isMounted(s3Uri));
    assertTrue(handler.isMounted(MetricsSystem.escape(new AlluxioURI(s3Uri))));
    assertTrue(handler.isMounted(MetricsSystem.escape(new AlluxioURI(s3Uri + "/"))));
    assertFalse(handler.isMounted(hdfsUri));
    assertFalse(handler.isMounted(MetricsSystem.escape(new AlluxioURI(hdfsUri))));
  }
}
