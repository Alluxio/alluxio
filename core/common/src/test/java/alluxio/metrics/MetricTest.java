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

package alluxio.metrics;

import static org.junit.Assert.assertEquals;

import alluxio.grpc.MetricType;
import alluxio.util.CommonUtils;

import org.junit.Test;

import java.util.Random;

/**
 * Tests {@link Metric}.
 */
public final class MetricTest {

  @Test
  public void proto() {
    Metric metric = createRandom();
    Metric other = Metric.fromProto(metric.toProto());
    checkEquality(metric, other);
  }

  @Test
  public void testFullNameParsing() {
    String fullName = "Client.metric.tag1:A::/.tag2:B:/.192_1_1_1|A";
    Metric metric = Metric.from(fullName, 1, MetricType.COUNTER);
    assertEquals(fullName, metric.getFullMetricName());
  }

  @Test
  public void testMetricNameWithTags() {
    assertEquals("metric.t1:v1.t2:v2:",
        Metric.getMetricNameWithTags("metric", "t1", "v1", "t2", "v2:"));
  }

  public void checkEquality(Metric a, Metric b) {
    assertEquals(a.getName(), b.getName());
    assertEquals(a.getInstanceType(), b.getInstanceType());
    assertEquals(a.getValue(), b.getValue(), 1e-15);
    assertEquals(a.getSource(), b.getSource());
    assertEquals(a.getFullMetricName(), b.getFullMetricName());
  }

  public static Metric createRandom() {
    Random random = new Random();
    int idx = random.nextInt(MetricsSystem.InstanceType.values().length);
    MetricsSystem.InstanceType instance = MetricsSystem.InstanceType.values()[idx];
    String hostname = CommonUtils.randomAlphaNumString(random.nextInt(10));
    String name = CommonUtils.randomAlphaNumString(random.nextInt(10));
    double value = random.nextLong();
    MetricType metricType = MetricType.forNumber(random.nextInt(MetricType.values().length));
    return new Metric(instance, hostname, metricType, name, value);
  }
}
