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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalFactory;
import alluxio.metrics.MetricsSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.web.MasterWebServer;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Matchers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

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
  private static final Map<String, List<Long>> NO_BLOCKS_ON_TIERS = ImmutableMap.of();

  private static final long UFS_SPACE_TOTAL = 100L;
  private static final long UFS_SPACE_USED = 100L;
  private static final long UFS_SPACE_FREE = 100L;
  private static final String TEST_PATH = "test://test";
  private static final Map<String, Long> WORKER1_TOTAL_BYTES_ON_TIERS = ImmutableMap.of("MEM", 10L,
      "SSD", 20L);
  private static final Map<String, Long> WORKER2_TOTAL_BYTES_ON_TIERS = ImmutableMap.of("MEM",
      1000L, "SSD", 2000L);
  private static final Map<String, Long> WORKER1_USED_BYTES_ON_TIERS = ImmutableMap.of("MEM", 1L,
      "SSD", 2L);
  private static final Map<String, Long> WORKER2_USED_BYTES_ON_TIERS = ImmutableMap.of("MEM", 100L,
      "SSD", 200L);

  private MasterProcess mMasterProcess;
  private BlockMaster mBlockMaster;
  private MasterRegistry mRegistry;
  private AlluxioMasterRestServiceHandler mHandler;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Rule
  public ConfigurationRule mConfigurationRule = new ConfigurationRule(new HashMap() {
    {
      put(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, TEST_PATH);
    }
  });

  @BeforeClass
  public static void beforeClass() throws Exception {
    String filesPinnedProperty =
        MetricsSystem.getMasterMetricName(DefaultFileSystemMaster.Metrics.FILES_PINNED);
    MetricsSystem.METRIC_REGISTRY.remove(filesPinnedProperty);
  }

  @Before
  public void before() throws Exception {
    mMasterProcess = mock(MasterProcess.class);
    ServletContext context = mock(ServletContext.class);
    mRegistry = new MasterRegistry();
    JournalFactory factory =
        new Journal.Factory(new URI(mTestFolder.newFolder().getAbsolutePath()));
    mBlockMaster = new BlockMasterFactory().create(mRegistry, factory);
    mRegistry.start(true);
    when(mMasterProcess.getMaster(BlockMaster.class)).thenReturn(mBlockMaster);
    when(context.getAttribute(MasterWebServer.ALLUXIO_MASTER_SERVLET_RESOURCE_KEY)).thenReturn(
        mMasterProcess);
    registerFileSystemMock();
    mHandler = new AlluxioMasterRestServiceHandler(context);
    // Register two workers
    long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    long worker2 = mBlockMaster.getWorkerId(NET_ADDRESS_2);
    List<String> tiers = Arrays.asList("MEM", "SSD");

    mBlockMaster.workerRegister(worker1, tiers, WORKER1_TOTAL_BYTES_ON_TIERS,
        WORKER1_USED_BYTES_ON_TIERS, NO_BLOCKS_ON_TIERS);
    mBlockMaster.workerRegister(worker2, tiers, WORKER2_TOTAL_BYTES_ON_TIERS,
        WORKER2_USED_BYTES_ON_TIERS, NO_BLOCKS_ON_TIERS);
  }

  private void registerFileSystemMock() throws IOException {
    UnderFileSystemFactory underFileSystemFactoryMock = mock(UnderFileSystemFactory.class);
    when(underFileSystemFactoryMock.supportsPath(anyString())).thenReturn(Boolean.FALSE);
    when(underFileSystemFactoryMock.supportsPath(TEST_PATH)).thenReturn(Boolean.TRUE);
    UnderFileSystem underFileSystemMock = mock(UnderFileSystem.class);
    when(underFileSystemMock.getSpace(TEST_PATH, UnderFileSystem.SpaceType.SPACE_FREE)).thenReturn(
        UFS_SPACE_FREE);
    when(underFileSystemMock.getSpace(TEST_PATH, UnderFileSystem.SpaceType.SPACE_TOTAL))
        .thenReturn(UFS_SPACE_TOTAL);
    when(underFileSystemMock.getSpace(TEST_PATH, UnderFileSystem.SpaceType.SPACE_USED)).thenReturn(
        UFS_SPACE_USED);
    when(underFileSystemFactoryMock.create(eq(TEST_PATH),
        Matchers.<UnderFileSystemConfiguration>any())).thenReturn(underFileSystemMock);
    UnderFileSystemFactoryRegistry.register(underFileSystemFactoryMock);
  }

  @After
  public void after() throws Exception {
    mRegistry.stop();
  }

  @Test
  public void getConfiguration() {
    Response response = mHandler.getConfiguration();
    try {
      assertNotNull("Response must be not null!", response);
      assertNotNull("Response must have a entry!", response.getEntity());
      assertTrue("Entry must be a SortedMap!", (response.getEntity() instanceof SortedMap));
      SortedMap<String, String> entry = (SortedMap<String, String>) response.getEntity();
      assertFalse("Properties Map must be not empty!", (entry.isEmpty()));
    } finally {
      response.close();
    }
  }

  @Test
  public void getRpcAddress() {
    when(mMasterProcess.getRpcAddress()).thenReturn(new InetSocketAddress("localhost", 8080));
    Response response = mHandler.getRpcAddress();
    try {
      assertNotNull("Response must be not null!", response);
      assertNotNull("Response must have a entry!", response.getEntity());
      assertEquals("Entry must be a String!", String.class, response.getEntity().getClass());
      String entry = (String) response.getEntity();
      assertEquals("\"localhost/127.0.0.1:8080\"", entry);
    } finally {
      response.close();
    }
  }

  @Test
  public void getMetrics() {
    final int FILES_PINNED_TEST_VALUE = 100;
    String filesPinnedProperty =
        MetricsSystem.getMasterMetricName(DefaultFileSystemMaster.Metrics.FILES_PINNED);
    Gauge<Integer> filesPinnedGauge = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return FILES_PINNED_TEST_VALUE;
      }
    };
    MetricSet mockMetricsSet = mock(MetricSet.class);
    Map<String, Metric> map = new HashMap<>();
    map.put(filesPinnedProperty, filesPinnedGauge);

    when(mockMetricsSet.getMetrics()).thenReturn(map);
    MetricsSystem.METRIC_REGISTRY.registerAll(mockMetricsSet);

    Response response = mHandler.getMetrics();
    try {
      assertNotNull("Response must be not null!", response);
      assertNotNull("Response must have a entry!", response.getEntity());
      assertTrue("Entry must be a SortedMap!", (response.getEntity() instanceof SortedMap));
      SortedMap<String, Long> metricsMap = (SortedMap<String, Long>) response.getEntity();
      assertFalse("Metrics Map must be not empty!", (metricsMap.isEmpty()));
      assertTrue("Map must contain key " + filesPinnedProperty + "!",
          metricsMap.containsKey(filesPinnedProperty));
      assertEquals(FILES_PINNED_TEST_VALUE, metricsMap.get(filesPinnedProperty).longValue());
    } finally {
      response.close();
    }
  }

  @Test
  public void getStartTimeMs() {
    when(mMasterProcess.getStartTimeMs()).thenReturn(100L);
    Response response = mHandler.getStartTimeMs();
    try {
      assertNotNull("Response must be not null!", response);
      assertNotNull("Response must have a entry!", response.getEntity());
      assertEquals("Entry must be a Long!", Long.class, response.getEntity().getClass());
      Long entry = (Long) response.getEntity();
      assertEquals(100L, entry.longValue());
    } finally {
      response.close();
    }
  }

  @Test
  public void getUptimeMs() {
    when(mMasterProcess.getUptimeMs()).thenReturn(100L);
    Response response = mHandler.getUptimeMs();
    try {
      assertNotNull("Response must be not null!", response);
      assertNotNull("Response must have a entry!", response.getEntity());
      assertEquals("Entry must be a Long!", Long.class, response.getEntity().getClass());
      Long entry = (Long) response.getEntity();
      assertEquals(100L, entry.longValue());
    } finally {
      response.close();
    }
  }

  @Test
  public void getVersion() {
    Response response = mHandler.getVersion();
    try {
      assertNotNull("Response must be not null!", response);
      assertNotNull("Response must have a entry!", response.getEntity());
      assertEquals("Entry must be a String!", String.class, response.getEntity().getClass());
      String entry = (String) response.getEntity();
      assertEquals("\"" + RuntimeConstants.VERSION + "\"", entry);
    } finally {
      response.close();
    }
  }

  @Test
  public void getCapacityBytes() {
    Response response = mHandler.getCapacityBytes();
    try {
      assertNotNull("Response must be not null!", response);
      assertNotNull("Response must have a entry!", response.getEntity());
      assertEquals("Entry must be a Long!", Long.class, response.getEntity().getClass());
      Long entry = (Long) response.getEntity();
      long sum = 0;
      for (Map.Entry<String, Long> entry1 : WORKER1_TOTAL_BYTES_ON_TIERS.entrySet()) {
        Long totalBytes = entry1.getValue();
        sum = sum + totalBytes;
      }
      for (Map.Entry<String, Long> entry1 : WORKER2_TOTAL_BYTES_ON_TIERS.entrySet()) {
        Long totalBytes = entry1.getValue();
        sum = sum + totalBytes;
      }
      assertEquals(sum, entry.longValue());
    } finally {
      response.close();
    }
  }

  @Test
  public void getUsedBytes() {
    Response response = mHandler.getUsedBytes();
    try {
      assertNotNull("Response must be not null!", response);
      assertNotNull("Response must have a entry!", response.getEntity());
      assertEquals("Entry must be a Long!", Long.class, response.getEntity().getClass());
      Long entry = (Long) response.getEntity();
      long sum = 0;
      for (Map.Entry<String, Long> entry1 : WORKER1_USED_BYTES_ON_TIERS.entrySet()) {
        Long totalBytes = entry1.getValue();
        sum = sum + totalBytes;
      }
      for (Map.Entry<String, Long> entry1 : WORKER2_USED_BYTES_ON_TIERS.entrySet()) {
        Long totalBytes = entry1.getValue();
        sum = sum + totalBytes;
      }
      assertEquals(sum, entry.longValue());
    } finally {
      response.close();
    }
  }

  @Test
  public void getFreeBytes() {
    Response response = mHandler.getFreeBytes();
    try {
      assertNotNull("Response must be not null!", response);
      assertNotNull("Response must have a entry!", response.getEntity());
      assertEquals("Entry must be a Long!", Long.class, response.getEntity().getClass());
      Long entry = (Long) response.getEntity();

      long usedSum = 0;
      for (Map.Entry<String, Long> entry1 : WORKER1_USED_BYTES_ON_TIERS.entrySet()) {
        Long totalBytes = entry1.getValue();
        usedSum = usedSum + totalBytes;
      }
      for (Map.Entry<String, Long> entry1 : WORKER2_USED_BYTES_ON_TIERS.entrySet()) {
        Long totalBytes = entry1.getValue();
        usedSum = usedSum + totalBytes;
      }

      long totalSum = 0;
      for (Map.Entry<String, Long> entry1 : WORKER1_TOTAL_BYTES_ON_TIERS.entrySet()) {
        Long totalBytes = entry1.getValue();
        totalSum = totalSum + totalBytes;
      }
      for (Map.Entry<String, Long> entry1 : WORKER2_TOTAL_BYTES_ON_TIERS.entrySet()) {
        Long totalBytes = entry1.getValue();
        totalSum = totalSum + totalBytes;
      }

      assertEquals(totalSum - usedSum, entry.longValue());
    } finally {
      response.close();
    }
  }

  @Test
  public void getUfsCapacityBytes() {
    Response response = mHandler.getUfsCapacityBytes();
    try {
      assertNotNull("Response must be not null!", response);
      assertNotNull("Response must have a entry!", response.getEntity());
      assertEquals("Entry must be a Long!", Long.class, response.getEntity().getClass());
      Long entry = (Long) response.getEntity();
      assertEquals(UFS_SPACE_TOTAL, entry.longValue());
    } finally {
      response.close();
    }
  }

  @Test
  public void getUfsUsedBytes() {
    Response response = mHandler.getUfsUsedBytes();
    try {
      assertNotNull("Response must be not null!", response);
      assertNotNull("Response must have a entry!", response.getEntity());
      assertEquals("Entry must be a Long!", Long.class, response.getEntity().getClass());
      Long entry = (Long) response.getEntity();
      assertEquals(UFS_SPACE_USED, entry.longValue());
    } finally {
      response.close();
    }
  }

  @Test
  public void getUfsFreeBytes() {
    Response response = mHandler.getUfsFreeBytes();
    try {
      assertNotNull("Response must be not null!", response);
      assertNotNull("Response must have a entry!", response.getEntity());
      assertEquals("Entry must be a Long!", Long.class, response.getEntity().getClass());
      Long entry = (Long) response.getEntity();
      assertEquals(UFS_SPACE_FREE, entry.longValue());
    } finally {
      response.close();
    }
  }

  @Test
  public void getWorkerCount() {
    Response response = mHandler.getWorkerCount();
    try {
      assertNotNull("Response must be not null!", response);
      assertNotNull("Response must have a entry!", response.getEntity());
      assertEquals("Entry must be a Integer!", Integer.class, response.getEntity().getClass());
      Integer entry = (Integer) response.getEntity();
      assertEquals(Integer.valueOf(2), entry);
    } finally {
      response.close();
    }
  }

  @Test
  public void getWorkerInfoList() {
    long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    long worker2 = mBlockMaster.getWorkerId(NET_ADDRESS_2);
    Set<Long> expected = new HashSet<>();
    expected.add(worker1);
    expected.add(worker2);
    Response response = mHandler.getWorkerInfoList();
    try {
      assertNotNull("Response must be not null!", response);
      assertNotNull("Response must have a entry!", response.getEntity());
      assertTrue("Entry must be a List!", (response.getEntity() instanceof List));
      @SuppressWarnings("unchecked")
      List<WorkerInfo> entry = (List<WorkerInfo>) response.getEntity();
      Set<Long> actual = new HashSet<>();
      for (WorkerInfo info : entry) {
        actual.add(info.getId());
      }
      assertEquals(expected, actual);
    } finally {
      response.close();
    }
  }
}
