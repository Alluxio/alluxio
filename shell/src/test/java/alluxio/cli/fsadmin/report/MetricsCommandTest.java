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

import alluxio.client.metrics.MetricsMasterClient;
import alluxio.grpc.MetricType;
import alluxio.grpc.MetricValue;
<<<<<<< HEAD
import alluxio.metrics.MetricKey;
=======
import alluxio.metrics.ClientMetrics;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.WorkerMetrics;
>>>>>>> 4653c7bc263386425095297458070bf026048aa3

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
  private MetricsMasterClient mMetricsMasterClient;
  private ByteArrayOutputStream mOutputStream;
  private PrintStream mPrintStream;

  @Before
  public void before() throws IOException {
    Map<String, MetricValue> metricsMap = generateMetricsMap();

    // Prepare mock meta master client
    mMetricsMasterClient = Mockito.mock(MetricsMasterClient.class);
    Mockito.when(mMetricsMasterClient.getMetrics()).thenReturn(metricsMap);

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
    MetricsCommand metricsCommand = new MetricsCommand(mMetricsMasterClient, mPrintStream);
    metricsCommand.run();
    checkIfOutputValid();
  }

  /**
   * @return a generated metrics map
   */
  private Map<String, MetricValue> generateMetricsMap() {
    Map<String, MetricValue> map = new HashMap<>();
<<<<<<< HEAD
    map.put(MetricKey.CLUSTER_BYTES_READ_LOCAL.getName(),
        MetricValue.newBuilder().setLongValue(12312312312L).build());
    map.put(MetricKey.CLUSTER_BYTES_READ_ALLUXIO.getName(),
        MetricValue.newBuilder().setLongValue(421312312L).build());
    map.put(MetricKey.CLUSTER_BYTES_READ_DOMAIN.getName(),
        MetricValue.newBuilder().setLongValue(4245232L).build());
    map.put(MetricKey.CLUSTER_BYTES_READ_UFS_ALL.getName(),
        MetricValue.newBuilder().setLongValue(534214123L).build());
    map.put(MetricKey.CLUSTER_BYTES_WRITTEN_ALLUXIO.getName(),
        MetricValue.newBuilder().setLongValue(23532L).build());
    map.put(MetricKey.CLUSTER_BYTES_WRITTEN_DOMAIN.getName(),
        MetricValue.newBuilder().setLongValue(65463532L).build());
    map.put(MetricKey.CLUSTER_BYTES_WRITTEN_UFS_ALL.getName(),
        MetricValue.newBuilder().setLongValue(325324L).build());

    map.put(MetricKey.CLUSTER_BYTES_READ_LOCAL_THROUGHPUT.getName(),
        MetricValue.newBuilder().setLongValue(123125324L).build());
    map.put(MetricKey.CLUSTER_BYTES_READ_ALLUXIO_THROUGHPUT.getName(),
        MetricValue.newBuilder().setLongValue(543534623L).build());
    map.put(MetricKey.CLUSTER_BYTES_READ_DOMAIN_THROUGHPUT.getName(),
        MetricValue.newBuilder().setLongValue(31423412L).build());
    map.put(MetricKey.CLUSTER_BYTES_READ_UFS_THROUGHPUT.getName(),
        MetricValue.newBuilder().setLongValue(745632L).build());
    map.put(MetricKey.CLUSTER_BYTES_WRITTEN_ALLUXIO_THROUGHPUT.getName(),
        MetricValue.newBuilder().setLongValue(8423432L).build());
    map.put(MetricKey.CLUSTER_BYTES_WRITTEN_DOMAIN_THROUGHPUT.getName(),
        MetricValue.newBuilder().setLongValue(1231231L).build());
    map.put(MetricKey.CLUSTER_BYTES_WRITTEN_UFS_THROUGHPUT.getName(),
        MetricValue.newBuilder().setLongValue(34264L).build());

    map.put(MetricKey.MASTER_DIRECTORIES_CREATED.getName(),
        MetricValue.newBuilder().setLongValue(121L).build());
    map.put(MetricKey.MASTER_FILE_BLOCK_INFOS_GOT.getName(),
        MetricValue.newBuilder().setLongValue(31243412L).build());
    map.put(MetricKey.MASTER_FILE_INFOS_GOT.getName(),
        MetricValue.newBuilder().setLongValue(12312321434276L).build());
    map.put(MetricKey.MASTER_FILES_COMPLETED.getName(),
        MetricValue.newBuilder().setLongValue(0L).build());
    map.put(MetricKey.MASTER_FILES_CREATED.getName(),
        MetricValue.newBuilder().setLongValue(534L).build());
    map.put(MetricKey.MASTER_FILES_FREED.getName(),
        MetricValue.newBuilder().setLongValue(2141L).build());
    map.put(MetricKey.MASTER_FILES_PERSISTED.getName(),
        MetricValue.newBuilder().setLongValue(4171L).build());
    map.put(MetricKey.MASTER_NEW_BLOCKS_GOT.getName(),
        MetricValue.newBuilder().setLongValue(4L).build());
    map.put(MetricKey.MASTER_PATHS_DELETED.getName(),
        MetricValue.newBuilder().setLongValue(583L).build());
    map.put(MetricKey.MASTER_PATHS_MOUNTED.getName(),
        MetricValue.newBuilder().setLongValue(3635L).build());
    map.put(MetricKey.MASTER_PATHS_RENAMED.getName(),
        MetricValue.newBuilder().setLongValue(382L).build());
    map.put(MetricKey.MASTER_PATHS_UNMOUNTED.getName(),
        MetricValue.newBuilder().setLongValue(975L).build());

    map.put(MetricKey.MASTER_COMPLETE_FILE_OPS.getName(),
        MetricValue.newBuilder().setLongValue(813L).build());
    map.put(MetricKey.MASTER_CREATE_DIRECTORIES_OPS.getName(),
        MetricValue.newBuilder().setLongValue(325728397L).build());
    map.put(MetricKey.MASTER_CREATE_FILES_OPS.getName(),
        MetricValue.newBuilder().setLongValue(89L).build());
    map.put(MetricKey.MASTER_DELETE_PATHS_OPS.getName(),
        MetricValue.newBuilder().setLongValue(21L).build());
    map.put(MetricKey.MASTER_FREE_FILE_OPS.getName(),
        MetricValue.newBuilder().setLongValue(5213L).build());
    map.put(MetricKey.MASTER_GET_FILE_BLOCK_INFO_OPS.getName(),
        MetricValue.newBuilder().setLongValue(798L).build());
    map.put(MetricKey.MASTER_GET_FILE_INFO_OPS.getName(),
        MetricValue.newBuilder().setLongValue(32L).build());
    map.put(MetricKey.MASTER_GET_NEW_BLOCK_OPS.getName(),
        MetricValue.newBuilder().setLongValue(912572136653L).build());
    map.put(MetricKey.MASTER_MOUNT_OPS.getName(),
        MetricValue.newBuilder().setLongValue(953795L).build());
    map.put(MetricKey.MASTER_RENAME_PATH_OPS.getName(),
        MetricValue.newBuilder().setLongValue(29L).build());
    map.put(MetricKey.MASTER_SET_ACL_OPS.getName(),
        MetricValue.newBuilder().setLongValue(316L).build());
    map.put(MetricKey.MASTER_SET_ATTRIBUTE_OPS.getName(),
        MetricValue.newBuilder().setLongValue(0L).build());
    map.put(MetricKey.MASTER_UNMOUNT_OPS.getName(),
        MetricValue.newBuilder().setLongValue(1L).build());

    map.put("UfsSessionCount-Ufs:_alluxio_underFSStorage",
        MetricValue.newBuilder().setLongValue(8535L).build());
    map.put("UfsSessionCount-Ufs:file:___Users_alluxio_alluxioMountedFolder",
        MetricValue.newBuilder().setLongValue(1231L).build());
    map.put("master.CapacityTotal",
        MetricValue.newBuilder().setLongValue(1154531246129122L).build());
=======
    map.put(MetricsSystem.getClusterMetricName(ClientMetrics.BYTES_READ_LOCAL),
        MetricValue.newBuilder().setDoubleValue(12312312312L).build());
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_ALLUXIO),
        MetricValue.newBuilder().setDoubleValue(421312312L).build());
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_DOMAIN),
        MetricValue.newBuilder().setDoubleValue(4245232L).build());
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_UFS_ALL),
        MetricValue.newBuilder().setDoubleValue(534214123L).build());
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_ALLUXIO),
        MetricValue.newBuilder().setDoubleValue(23532L).build());
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_DOMAIN),
        MetricValue.newBuilder().setDoubleValue(65463532L).build());
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_UFS_ALL),
        MetricValue.newBuilder().setDoubleValue(325324L).build());

    map.put(MetricsSystem.getClusterMetricName(ClientMetrics.BYTES_READ_LOCAL_THROUGHPUT),
        MetricValue.newBuilder().setDoubleValue(123125324L).build());
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_ALLUXIO_THROUGHPUT),
        MetricValue.newBuilder().setDoubleValue(543534623L).build());
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_DOMAIN_THROUGHPUT),
        MetricValue.newBuilder().setDoubleValue(31423412L).build());
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_UFS_THROUGHPUT),
        MetricValue.newBuilder().setDoubleValue(745632L).build());
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_ALLUXIO_THROUGHPUT),
        MetricValue.newBuilder().setDoubleValue(8423432L).build());
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_DOMAIN_THROUGHPUT),
        MetricValue.newBuilder().setDoubleValue(1231231L).build());
    map.put(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_UFS_THROUGHPUT),
        MetricValue.newBuilder().setDoubleValue(34264L).build());

    map.put("Master.CompleteFileOps",
        MetricValue.newBuilder().setMetricType(MetricType.COUNTER).setDoubleValue(813).build());
    map.put("Master.UfsSessionCount-Ufs:_alluxio_underFSStorage",
        MetricValue.newBuilder().setMetricType(MetricType.COUNTER).setDoubleValue(8535L).build());
    map.put("Master.UfsSessionCount-Ufs:file:___Users_alluxio_alluxioMountedFolder",
        MetricValue.newBuilder().setMetricType(MetricType.COUNTER).setDoubleValue(1231L).build());

    map.put("Master.CapacityTotal",
        MetricValue.newBuilder().setDoubleValue(1154531246129122L).build());
    map.put("Master.getMetrics.User:alluxio", MetricValue.newBuilder()
        .setMetricType(MetricType.TIMER).setDoubleValue(4).build());

>>>>>>> 4653c7bc263386425095297458070bf026048aa3
    map.put("heap.used", MetricValue.newBuilder().setDoubleValue(0.0028321312).build());
    map.put("pools.Metaspace.usage", MetricValue.newBuilder().setDoubleValue(0.95728).build());
    map.put("pools.Metaspace.max", MetricValue.newBuilder().setDoubleValue(-1).build());
    map.put("vendor", MetricValue.newBuilder().setMetricType(MetricType.GAUGE)
        .setStringValue("AdoptOpenJDK OpenJDK 64-Bit Server VM 25.222-b10 (1.8)").build());
    return map;
  }

  /**
   * Checks if the output is expected.
   */
  private void checkIfOutputValid() {
    String output = new String(mOutputStream.toByteArray(), StandardCharsets.UTF_8);
    // CHECKSTYLE.OFF: LineLengthExceed - Much more readable
    List<String> expectedOutput = Arrays.asList(
        "Cluster.BytesReadAlluxio  (Type: GAUGE, Value: 401.79MB)",
        "Cluster.BytesReadAlluxioThroughput  (Type: GAUGE, Value: 518.36MB/min)",
        "Cluster.BytesReadDomain  (Type: GAUGE, Value: 4145.73KB)",
        "Cluster.BytesReadDomainThroughput  (Type: GAUGE, Value: 29.97MB/min)",
        "Cluster.BytesReadLocal  (Type: GAUGE, Value: 11.47GB)",
        "Cluster.BytesReadLocalThroughput  (Type: GAUGE, Value: 117.42MB/min)",
        "Cluster.BytesReadUfsAll  (Type: GAUGE, Value: 509.47MB)",
        "Cluster.BytesReadUfsThroughput  (Type: GAUGE, Value: 728.16KB/min)",
        "Cluster.BytesWrittenAlluxio  (Type: GAUGE, Value: 22.98KB)",
        "Cluster.BytesWrittenAlluxioThroughput  (Type: GAUGE, Value: 8.03MB/min)",
        "Cluster.BytesWrittenDomain  (Type: GAUGE, Value: 62.43MB)",
        "Cluster.BytesWrittenDomainThroughput  (Type: GAUGE, Value: 1202.37KB/min)",
        "Cluster.BytesWrittenUfsAll  (Type: GAUGE, Value: 317.70KB)",
        "Cluster.BytesWrittenUfsThroughput  (Type: GAUGE, Value: 33.46KB/min)",
        "Master.CapacityTotal  (Type: GAUGE, Value: 1,154,531,246,129,122)",
        "Master.CompleteFileOps  (Type: COUNTER, Value: 813)",
        "Master.UfsSessionCount-Ufs:_alluxio_underFSStorage  (Type: COUNTER, Value: 8,535)",
        "Master.UfsSessionCount-Ufs:file:___Users_alluxio_alluxioMountedFolder  (Type: COUNTER, Value: 1,231)",
        "Master.getMetrics.User:alluxio  (Type: TIMER, Value: 4)");
    // CHECKSTYLE.ON: LineLengthExceed
    List<String> testOutput = Arrays.asList(output.split("\n"));
    Assert.assertThat(testOutput,
        IsIterableContainingInOrder.contains(expectedOutput.toArray()));
  }
}
