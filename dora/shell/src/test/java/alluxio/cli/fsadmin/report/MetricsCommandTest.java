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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import alluxio.client.metrics.MetricsMasterClient;
import alluxio.grpc.MetricType;
import alluxio.grpc.MetricValue;
import alluxio.metrics.MetricKey;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
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
    checkOutput();
  }

  /**
   * @return a generated metrics map
   */
  private Map<String, MetricValue> generateMetricsMap() {
    Map<String, MetricValue> map = new HashMap<>();

    map.put(MetricKey.CLUSTER_BYTES_READ_REMOTE.getName(),
        MetricValue.newBuilder().setDoubleValue(421312312L).build());
    map.put(MetricKey.CLUSTER_BYTES_READ_DOMAIN.getName(),
        MetricValue.newBuilder().setDoubleValue(4245232L).build());
    map.put(MetricKey.CLUSTER_BYTES_READ_UFS_ALL.getName(),
        MetricValue.newBuilder().setDoubleValue(534214123L).build());
    map.put(MetricKey.CLUSTER_BYTES_WRITTEN_REMOTE.getName(),
        MetricValue.newBuilder().setDoubleValue(23532L).build());
    map.put(MetricKey.CLUSTER_BYTES_WRITTEN_DOMAIN.getName(),
        MetricValue.newBuilder().setDoubleValue(65463532L).build());
    map.put(MetricKey.CLUSTER_BYTES_WRITTEN_UFS_ALL.getName(),
        MetricValue.newBuilder().setDoubleValue(325324L).build());

    map.put(MetricKey.CLUSTER_BYTES_READ_REMOTE_THROUGHPUT.getName(),
        MetricValue.newBuilder().setDoubleValue(543534623L).build());
    map.put(MetricKey.CLUSTER_BYTES_READ_DOMAIN_THROUGHPUT.getName(),
        MetricValue.newBuilder().setDoubleValue(31423412L).build());
    map.put(MetricKey.CLUSTER_BYTES_READ_UFS_THROUGHPUT.getName(),
        MetricValue.newBuilder().setDoubleValue(745632L).build());
    map.put(MetricKey.CLUSTER_BYTES_WRITTEN_REMOTE_THROUGHPUT.getName(),
        MetricValue.newBuilder().setDoubleValue(8423432L).build());
    map.put(MetricKey.CLUSTER_BYTES_WRITTEN_DOMAIN_THROUGHPUT.getName(),
        MetricValue.newBuilder().setDoubleValue(1231231L).build());
    map.put(MetricKey.CLUSTER_BYTES_WRITTEN_UFS_THROUGHPUT.getName(),
        MetricValue.newBuilder().setDoubleValue(34264L).build());

    map.put(MetricKey.MASTER_COMPLETE_FILE_OPS.getName(),
        MetricValue.newBuilder().setMetricType(MetricType.COUNTER).setDoubleValue(813).build());
    map.put("Master.UfsSessionCount-Ufs:_alluxio_underFSStorage",
        MetricValue.newBuilder().setMetricType(MetricType.COUNTER).setDoubleValue(8535L).build());
    map.put("Master.UfsSessionCount-Ufs:file:___Users_alluxio_alluxioMountedFolder",
        MetricValue.newBuilder().setMetricType(MetricType.COUNTER).setDoubleValue(1231L).build());

    map.put(MetricKey.CLUSTER_CAPACITY_TOTAL.getName(),
        MetricValue.newBuilder().setDoubleValue(1154531246129122L).build());
    map.put("Master.getMetrics.User:alluxio", MetricValue.newBuilder()
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
  private void checkOutput() throws JsonProcessingException {
    String output = new String(mOutputStream.toByteArray(), StandardCharsets.UTF_8);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode metricsInfo = mapper.readTree(output).get("metricsInfo");
    assertEquals(17, metricsInfo.size());
    for (JsonNode metricInfo : metricsInfo) {
      switch (metricInfo.get("key").asText()) {
        case "Cluster.BytesReadDomain":
          assertEquals("GAUGE", metricInfo.get("type").asText());
          assertEquals("4145.73KB", metricInfo.get("value").asText());
          break;
        case "Cluster.BytesReadDomainThroughput":
          assertEquals("GAUGE", metricInfo.get("type").asText());
          assertEquals("29.97MB/MIN", metricInfo.get("value").asText());
          break;
        case "Cluster.BytesReadRemote":
          assertEquals("GAUGE", metricInfo.get("type").asText());
          assertEquals("401.79MB", metricInfo.get("value").asText());
          break;
        case "Cluster.BytesReadRemoteThroughput":
          assertEquals("GAUGE", metricInfo.get("type").asText());
          assertEquals("518.36MB/MIN", metricInfo.get("value").asText());
          break;
        case "Cluster.BytesReadUfsAll":
          assertEquals("GAUGE", metricInfo.get("type").asText());
          assertEquals("509.47MB", metricInfo.get("value").asText());
          break;
        case "Cluster.BytesReadUfsThroughput":
          assertEquals("GAUGE", metricInfo.get("type").asText());
          assertEquals("728.16KB/MIN", metricInfo.get("value").asText());
          break;
        case "Cluster.BytesWrittenDomain":
          assertEquals("GAUGE", metricInfo.get("type").asText());
          assertEquals("62.43MB", metricInfo.get("value").asText());
          break;
        case "Cluster.BytesWrittenDomainThroughput":
          assertEquals("GAUGE", metricInfo.get("type").asText());
          assertEquals("1202.37KB/MIN", metricInfo.get("value").asText());
          break;
        case "Cluster.BytesWrittenRemote":
          assertEquals("GAUGE", metricInfo.get("type").asText());
          assertEquals("22.98KB", metricInfo.get("value").asText());
          break;
        case "Cluster.BytesWrittenRemoteThroughput":
          assertEquals("GAUGE", metricInfo.get("type").asText());
          assertEquals("8.03MB/MIN", metricInfo.get("value").asText());
          break;
        case "Cluster.BytesWrittenUfsAll":
          assertEquals("GAUGE", metricInfo.get("type").asText());
          assertEquals("317.70KB", metricInfo.get("value").asText());
          break;
        case "Cluster.BytesWrittenUfsThroughput":
          assertEquals("GAUGE", metricInfo.get("type").asText());
          assertEquals("33.46KB/MIN", metricInfo.get("value").asText());
          break;
        case "Cluster.CapacityTotal":
          assertEquals("GAUGE", metricInfo.get("type").asText());
          assertEquals("1,154,531,246,129,122", metricInfo.get("value").asText());
          break;
        case "Master.CompleteFileOps":
          assertEquals("COUNTER", metricInfo.get("type").asText());
          assertEquals("813", metricInfo.get("value").asText());
          break;
        case "Master.UfsSessionCount-Ufs:_alluxio_underFSStorage":
          assertEquals("COUNTER", metricInfo.get("type").asText());
          assertEquals("8,535", metricInfo.get("value").asText());
          break;
        case "Master.UfsSessionCount-Ufs:file:___Users_alluxio_alluxioMountedFolder":
          assertEquals("COUNTER", metricInfo.get("type").asText());
          assertEquals("1,231", metricInfo.get("value").asText());
          break;
        case "Master.getMetrics.User:alluxio":
          assertEquals("TIMER", metricInfo.get("type").asText());
          assertEquals("4", metricInfo.get("value").asText());
          break;
        default:
          fail("Invalid key: " + metricInfo.get("key").asText());
      }
    }
  }
}
