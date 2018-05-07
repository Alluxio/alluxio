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

import alluxio.util.CommonUtils;

import org.junit.Test;

import java.util.Random;

/**
 * Tests {@link Metric}.
 */
public class MetricTest {

  @Test
  public void thrift() {
    Metric metric = createRandom();
    Metric other = Metric.from(metric.toThrift());
    checkEquality(metric, other);
  }

  public void checkEquality(Metric a, Metric b) {
    assertEquals(a.getName(), b.getName());
    assertEquals(a.getInstanceType(), b.getInstanceType());
    assertEquals(a.getValue(), b.getValue());
    assertEquals(a.getHostname(), b.getHostname());
    assertEquals(a.getFullMetricName(), b.getFullMetricName());
  }

  public static Metric createRandom() {
    Random random = new Random();
    int idx = random.nextInt(MetricsSystem.InstanceType.values().length);
    MetricsSystem.InstanceType instance = MetricsSystem.InstanceType.values()[idx];
    String hostname = CommonUtils.randomAlphaNumString(random.nextInt(10));
    String name = CommonUtils.randomAlphaNumString(random.nextInt(10));
    long value = random.nextLong();
    return new Metric(instance, hostname, name, value);
  }
}
