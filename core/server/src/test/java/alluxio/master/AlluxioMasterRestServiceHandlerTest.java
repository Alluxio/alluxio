/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import alluxio.RuntimeConstants;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.Journal;
import alluxio.metrics.MetricsSystem;
import alluxio.wire.WorkerInfo;
import alluxio.web.MasterUIWebServer;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;

import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.List;
import java.util.SortedMap;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.Response;
import javax.servlet.ServletContext;

/**
 * Unit tests for {@link AlluxioMasterRestServiceHandler}.
 */

public class AlluxioMasterRestServiceHandlerTest {
  private AlluxioMaster mMaster;
  private ServletContext mContext;
  private BlockMaster mBlockMaster;
  private AlluxioMasterRestServiceHandler mHandler;

  @BeforeClass
  public static void setUpClass() {
    String filesPinnedProperty =
        MetricsSystem.getMasterMetricName(FileSystemMaster.Metrics.FILES_PINNED);
    MetricsSystem.METRIC_REGISTRY.remove(filesPinnedProperty);
  }

  @Before
  public void setUp() {
    mMaster = mock(AlluxioMaster.class);
    mContext = mock(ServletContext.class);
    Journal journal = mock(Journal.class);
    mBlockMaster = new BlockMaster(journal);
    when(mMaster.getBlockMaster()).thenReturn(mBlockMaster);
    when(mMaster.getMasterAddress()).thenReturn(new InetSocketAddress("localhost", 8080));
    when(mContext.getAttribute(MasterUIWebServer.ALLUXIO_MASTER_SERVLET_RESOURCE_KEY)).thenReturn(
        mMaster);
    mHandler = new AlluxioMasterRestServiceHandler(mContext);
  }

  @Test
  public void testGetConfiguration() {
    Response response = mHandler.getConfiguration();
    assertNotNull("Response must be not null!", response);
    assertNotNull("Response must have a entry!", response.getEntity());
    assertTrue("Entry must be a SortedMap!", (response.getEntity() instanceof SortedMap));
    SortedMap<String, String> entry = (SortedMap<String, String>) response.getEntity();
    assertFalse("Properties Map must be not empty!", (entry.isEmpty()));
  }

  @Test
  public void testGetRpcAddress() {
    Response response = mHandler.getRpcAddress();
    assertNotNull("Response must be not null!", response);
    assertNotNull("Response must have a entry!", response.getEntity());
    assertEquals("Entry must be a String!", String.class, response.getEntity().getClass());
    String entry = (String) response.getEntity();
    assertFalse("Properties Map must be not empty!", (entry.isEmpty()));
  }

  @Test
  public void testGetMetrics() {
    final int FILES_PINNED_TEST_VALUE = 100;
    String filesPinnedProperty =
        MetricsSystem.getMasterMetricName(FileSystemMaster.Metrics.FILES_PINNED);
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
    assertNotNull("Response must be not null!", response);
    assertNotNull("Response must have a entry!", response.getEntity());
    assertTrue("Entry must be a SortedMap!", (response.getEntity() instanceof SortedMap));
    SortedMap<String, Long> entry = (SortedMap<String, Long>) response.getEntity();
    assertFalse("Properties Map must be not empty!", (entry.isEmpty()));
    assertTrue("Map must contains key " + filesPinnedProperty + "!",
        entry.containsKey(filesPinnedProperty));
    assertEquals(FILES_PINNED_TEST_VALUE, entry.get(filesPinnedProperty).longValue());
  }

  @Test
  public void testGetStartTimeMs() {
    Response response = mHandler.getStartTimeMs();
    assertNotNull("Response must be not null!", response);
    assertNotNull("Response must have a entry!", response.getEntity());
    assertEquals("Entry must be a Long!", Long.class, response.getEntity().getClass());
    Long entry = (Long) response.getEntity();
    assertEquals(0L, entry.longValue());
  }

  @Test
  public void testGetUptimeMs() {
    Response response = mHandler.getUptimeMs();
    assertNotNull("Response must be not null!", response);
    assertNotNull("Response must have a entry!", response.getEntity());
    assertEquals("Entry must be a Long!", Long.class, response.getEntity().getClass());
    Long entry = (Long) response.getEntity();
    assertEquals(0L, entry.longValue());
  }

  @Test
  public void testGetVersion() {
    Response response = mHandler.getVersion();
    assertNotNull("Response must be not null!", response);
    assertNotNull("Response must have a entry!", response.getEntity());
    assertEquals("Entry must be a String!", String.class, response.getEntity().getClass());
    String entry = (String) response.getEntity();
    assertEquals("\"" + RuntimeConstants.VERSION + "\"", entry);
  }

  @Test
  public void testGetCapacityBytes() {
    Response response = mHandler.getCapacityBytes();
    assertNotNull("Response must be not null!", response);
    assertNotNull("Response must have a entry!", response.getEntity());
    assertEquals("Entry must be a Long!", Long.class, response.getEntity().getClass());
    Long entry = (Long) response.getEntity();
    assertEquals(0L, entry.longValue());
  }

  @Test
  public void testGetUsedBytes() {
    Response response = mHandler.getUsedBytes();
    assertNotNull("Response must be not null!", response);
    assertNotNull("Response must have a entry!", response.getEntity());
    assertEquals("Entry must be a Long!", Long.class, response.getEntity().getClass());
    Long entry = (Long) response.getEntity();
    assertEquals(0L, entry.longValue());
  }

  @Test
  public void testGetFreeBytes() {
    Response response = mHandler.getFreeBytes();
    assertNotNull("Response must be not null!", response);
    assertNotNull("Response must have a entry!", response.getEntity());
    assertEquals("Entry must be a Long!", Long.class, response.getEntity().getClass());
    Long entry = (Long) response.getEntity();
    assertEquals(0L, entry.longValue());
  }

  @Test
  public void testGetUfsCapacityBytes() {
    Response response = mHandler.getUfsCapacityBytes();
    assertNotNull("Response must be not null!", response);
    assertNotNull("Response must have a entry!", response.getEntity());
    assertEquals("Entry must be a Long!", Long.class, response.getEntity().getClass());
    Long entry = (Long) response.getEntity();
    assertEquals(0L, entry.longValue());
  }

  @Test
  public void testGetUfsUsedBytes() {
    Response response = mHandler.getUfsUsedBytes();
    assertNotNull("Response must be not null!", response);
    assertNotNull("Response must have a entry!", response.getEntity());
    assertEquals("Entry must be a Long!", Long.class, response.getEntity().getClass());
    Long entry = (Long) response.getEntity();
    assertEquals(0L, entry.longValue());
  }

  @Test
  public void testGetUfsFreeBytes() {
    Response response = mHandler.getUfsFreeBytes();
    assertNotNull("Response must be not null!", response);
    assertNotNull("Response must have a entry!", response.getEntity());
    assertEquals("Entry must be a Long!", Long.class, response.getEntity().getClass());
    Long entry = (Long) response.getEntity();
    assertEquals(0L, entry.longValue());
  }

  @Test
  public void testGetWorkerCount() {
    Response response = mHandler.getWorkerCount();
    assertNotNull("Response must be not null!", response);
    assertNotNull("Response must have a entry!", response.getEntity());
    assertEquals("Entry must be a Integer!", Integer.class, response.getEntity().getClass());
    Integer entry = (Integer) response.getEntity();
    assertEquals(Integer.valueOf(0), entry);
  }

  @Test
  public void testGetWorkerInfoList() {
    Response response = mHandler.getWorkerInfoList();
    assertNotNull("Response must be not null!", response);
    assertNotNull("Response must have a entry!", response.getEntity());
    assertTrue("Entry must be a List!", (response.getEntity() instanceof List));
    @SuppressWarnings("unchecked")
    List<WorkerInfo> entry = (List<WorkerInfo>) response.getEntity();
    assertTrue(entry.isEmpty());
  }
}
