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
import alluxio.metrics.ClientMetrics;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.WorkerMetrics;

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

    map.put("master.CompleteFileOps",
        MetricValue.newBuilder().setMetricType(MetricType.COUNTER).setDoubleValue(813).build());
    map.put("UfsSessionCount-Ufs:_alluxio_underFSStorage",
        MetricValue.newBuilder().setMetricType(MetricType.COUNTER).setDoubleValue(8535L).build());
    map.put("UfsSessionCount-Ufs:file:___Users_alluxio_alluxioMountedFolder",
        MetricValue.newBuilder().setMetricType(MetricType.COUNTER).setDoubleValue(1231L).build());

    map.put("master.CapacityTotal",
        MetricValue.newBuilder().setDoubleValue(1154531246129122L).build());
    map.put("master.getMetrics.User:alluxio", MetricValue.newBuilder()
        .setMetricType(MetricType.TIMER).setDoubleValue(4).build());

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
        "UfsSessionCount-Ufs:_alluxio_underFSStorage  (Type: COUNTER, Value: 8,535)",
        "UfsSessionCount-Ufs:file:___Users_alluxio_alluxioMountedFolder  (Type: COUNTER, Value: 1,231)",
        "cluster.BytesReadAlluxio  (Type: GAUGE, Value: 401.79MB)",
        "cluster.BytesReadAlluxioThroughput  (Type: GAUGE, Value: 518.36MB/min)",
        "cluster.BytesReadDomain  (Type: GAUGE, Value: 4145.73KB)",
        "cluster.BytesReadDomainThroughput  (Type: GAUGE, Value: 29.97MB/min)",
        "cluster.BytesReadLocal  (Type: GAUGE, Value: 11.47GB)",
        "cluster.BytesReadLocalThroughput  (Type: GAUGE, Value: 117.42MB/min)",
        "cluster.BytesReadUfsAll  (Type: GAUGE, Value: 509.47MB)",
        "cluster.BytesReadUfsThroughput  (Type: GAUGE, Value: 728.16KB/min)",
        "cluster.BytesWrittenAlluxio  (Type: GAUGE, Value: 22.98KB)",
        "cluster.BytesWrittenAlluxioThroughput  (Type: GAUGE, Value: 8.03MB/min)",
        "cluster.BytesWrittenDomain  (Type: GAUGE, Value: 62.43MB)",
        "cluster.BytesWrittenDomainThroughput  (Type: GAUGE, Value: 1202.37KB/min)",
        "cluster.BytesWrittenUfsAll  (Type: GAUGE, Value: 317.70KB)",
        "cluster.BytesWrittenUfsThroughput  (Type: GAUGE, Value: 33.46KB/min)",
        "heap.used  (Type: GAUGE, Value: 0.0028321312)",
        "master.CapacityTotal  (Type: GAUGE, Value: 1,154,531,246,129,122)",
        "master.CompleteFileOps  (Type: COUNTER, Value: 813)",
        "master.getMetrics.User:alluxio  (Type: TIMER, Value: 4)",
        "pools.Metaspace.max  (Type: GAUGE, Value: -1)",
        "pools.Metaspace.usage  (Type: GAUGE, Value: 0.95728)",
        "vendor  (Type: GAUGE, Value: AdoptOpenJDK OpenJDK 64-Bit Server VM 25.222-b10 (1.8))");
    // CHECKSTYLE.ON: LineLengthExceed
    System.out.println(output);
    List<String> testOutput = Arrays.asList(output.split("\n"));
    Assert.assertThat(testOutput,
        IsIterableContainingInOrder.contains(expectedOutput.toArray()));
  }
}
