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

package alluxio.cli.fsadmin.report;

import alluxio.client.MetaMasterClient;
import alluxio.metrics.ClientMetrics;
import alluxio.metrics.MasterMetrics;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.WorkerMetrics;
import alluxio.wire.MetricValue;

import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetricsCommandTest {
  private MetaMasterClient mMetaMasterClient;
  private ByteArrayOutputStream mOutputStream;
  private PrintStream mPrintStream;

  @Before
  public void before() throws IOException {
    Map<String, MetricValue> metricsMap = generateMetricsMap();

    // Prepare mock meta master client
    mMetaMasterClient = Mockito.mock(MetaMasterClient.class);
    Mockito.when(mMetaMasterClient.getMetrics()).thenReturn(metricsMap);

    // Prepare print stream
    mOutputStream = new ByteArrayOutputStream();
    mPrintStream = new PrintStream(mOutputStream, true, "utf-8");
  }

  @After
  public void after() {
    mPrintStream.close();
  }

  @Test
  public void metrics() throws IOException {
    MetricsCommand metricsCommand = new MetricsCommand(mMetaMasterClient, mPrintStream);
    metricsCommand.run();
    checkIfOutputValid();
  }

  /**
   * @return a generated metrics map
   */
  private Map<String, MetricValue> generateMetricsMap() {
    Map<String, MetricValue> map = new HashMap<>();
    map.put(MetricsSystem.getClusterMetricName(ClientMetrics.BYTES_READ_LOCAL),
        MetricValue.forLong(12312312312L));
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_ALLUXIO),
        MetricValue.forLong(421312312L));
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_UFS_ALL),
        MetricValue.forLong(534214123L));
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_ALLUXIO),
        MetricValue.forLong(23532L));
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_UFS_ALL),
        MetricValue.forLong(325324L));

    map.put(MetricsSystem.getClusterMetricName(ClientMetrics.BYTES_READ_LOCAL_THROUGHPUT),
        MetricValue.forLong(123125324L));
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_ALLUXIO_THROUGHPUT),
        MetricValue.forLong(543534623L));
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_UFS_THROUGHPUT),
        MetricValue.forLong(745632L));
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_ALLUXIO_THROUGHPUT),
        MetricValue.forLong(8423432L));
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_UFS_THROUGHPUT),
        MetricValue.forLong(34264L));

    map.put(MasterMetrics.DIRECTORIES_CREATED, MetricValue.forLong(121L));
    map.put(MasterMetrics.FILE_BLOCK_INFOS_GOT, MetricValue.forLong(31243412L));
    map.put(MasterMetrics.FILE_INFOS_GOT, MetricValue.forLong(12312321434276L));
    map.put(MasterMetrics.FILES_COMPLETED, MetricValue.forLong(0L));
    map.put(MasterMetrics.FILES_CREATED, MetricValue.forLong(534L));
    map.put(MasterMetrics.FILES_FREED, MetricValue.forLong(2141L));
    map.put(MasterMetrics.FILES_PERSISTED, MetricValue.forLong(4171L));
    map.put(MasterMetrics.NEW_BLOCKS_GOT, MetricValue.forLong(4L));
    map.put(MasterMetrics.PATHS_DELETED, MetricValue.forLong(583L));
    map.put(MasterMetrics.PATHS_MOUNTED, MetricValue.forLong(3635L));
    map.put(MasterMetrics.PATHS_RENAMED, MetricValue.forLong(382L));
    map.put(MasterMetrics.PATHS_UNMOUNTED, MetricValue.forLong(975L));

    map.put(MasterMetrics.COMPLETE_FILE_OPS, MetricValue.forLong(813L));
    map.put(MasterMetrics.CREATE_DIRECTORIES_OPS, MetricValue.forLong(325728397L));
    map.put(MasterMetrics.CREATE_FILES_OPS, MetricValue.forLong(89L));
    map.put(MasterMetrics.DELETE_PATHS_OPS, MetricValue.forLong(21L));
    map.put(MasterMetrics.FREE_FILE_OPS, MetricValue.forLong(5213L));
    map.put(MasterMetrics.GET_FILE_BLOCK_INFO_OPS, MetricValue.forLong(798L));
    map.put(MasterMetrics.GET_FILE_INFO_OPS, MetricValue.forLong(32L));
    map.put(MasterMetrics.GET_NEW_BLOCK_OPS, MetricValue.forLong(912572136653L));
    map.put(MasterMetrics.MOUNT_OPS, MetricValue.forLong(953795L));
    map.put(MasterMetrics.RENAME_PATH_OPS, MetricValue.forLong(29L));
    map.put(MasterMetrics.SET_ATTRIBUTE_OPS, MetricValue.forLong(0L));
    map.put(MasterMetrics.UNMOUNT_OPS, MetricValue.forLong(1L));

    map.put("UfsSessionCount-Ufs:_alluxio_underFSStorage",
        MetricValue.forLong(8535L));
    map.put("UfsSessionCount-Ufs:file:___Users_alluxio_alluxioMountedFolder",
        MetricValue.forLong(1231L));
    map.put("master.CapacityTotal", MetricValue.forLong(1154531246129122L));
    map.put("heap.used", MetricValue.forDouble(0.0028321312));
    map.put("pools.Metaspace.usage", MetricValue.forDouble(0.95728));
    map.put("pools.Metaspace.max", MetricValue.forLong(-1));
    return map;
  }

  /**
   * Checks if the output is expected.
   */
  private void checkIfOutputValid() {
    String output = new String(mOutputStream.toByteArray(), StandardCharsets.UTF_8);
    // CHECKSTYLE.OFF: LineLengthExceed - Much more readable
    List<String> expectedOutput = Arrays.asList(
        "Total IO Size: ",
        "    Short-circuit Read                          11.47GB",
        "    From Remote Instances                      401.79MB",
        "    Under Filesystem Read                      509.47MB",
        "    Alluxio Write                               22.98KB",
        "    Under Filesystem Write                     317.70KB",
        "",
        "Total IO Throughput (Last Minute): ",
        "    Short-circuit Read                         117.42MB",
        "    From Remote Instances                      518.36MB",
        "    Under Filesystem Read                      728.16KB",
        "    Alluxio Write                                8.03MB",
        "    Under Filesystem Write                      33.46KB",
        "",
        "Cache Hit Rate (Percentage): ",
        "    Alluxio Local                                 92.80",
        "    Alluxio Remote                                 3.18",
        "    Miss                                           4.03",
        "",
        "Logical Operations: ",
        "    Directories Created                             121",
        "    File Block Infos Got                     31,243,412",
        "    File Infos Got                   12,312,321,434,276",
        "    Files Completed                                   0",
        "    Files Created                                   534",
        "    Files Freed                                   2,141",
        "    Files Persisted                               4,171",
        "    New Blocks Got                                    4",
        "    Paths Deleted                                   583",
        "    Paths Mounted                                 3,635",
        "    Paths Renamed                                   382",
        "    Paths Unmounted                                 975",
        "",
        "RPC Invocations: ",
        "    Complete File Operations                        813",
        "    Create Directory Operations             325,728,397",
        "    Create File Operations                           89",
        "    Delete Path Operations                           21",
        "    Free File Operations                          5,213",
        "    Get File Block Info Operations                  798",
        "    Get File Info Operations                         32",
        "    Get New Block Operations            912,572,136,653",
        "    Mount Operations                            953,795",
        "    Rename Path Operations                           29",
        "    Set Attribute Operations                          0",
        "    Unmount Operations                                1",
        "",
        "Other metrics information: ",
        "    UfsSessionCount-Ufs:_alluxio_underFSStorage  (8,535)",
        "    UfsSessionCount-Ufs:file:___Users_alluxio_alluxioMountedFolder  (1,231)",
        "    heap.used  (0.00283)",
        "    master.CapacityTotal  (1,154,531,246,129,122)",
        "    pools.Metaspace.max  (-1)",
        "    pools.Metaspace.usage  (0.95728)");
    // CHECKSTYLE.ON: LineLengthExceed
    List<String> testOutput = Arrays.asList(output.split("\n"));
    Assert.assertThat(testOutput,
        IsIterableContainingInOrder.contains(expectedOutput.toArray()));
  }
}
