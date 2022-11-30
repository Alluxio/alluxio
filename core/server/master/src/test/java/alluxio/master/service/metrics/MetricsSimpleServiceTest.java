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

package alluxio.master.service.metrics;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MetricsSystem;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for Metrics simple service.
 */
public class MetricsSimpleServiceTest {
  @Test
  public void alwaysOnTest() {
    Configuration.set(PropertyKey.STANDBY_MASTER_METRICS_SINK_ENABLED, true);
    MetricsSimpleService service = MetricsSimpleService.Factory.create();
    Assert.assertTrue(service instanceof AlwaysOnMetricsSimpleService);

    Assert.assertFalse(MetricsSystem.isStarted());
    service.start();
    Assert.assertTrue(MetricsSystem.isStarted());
    for (int i = 0; i < 5; i++) {
      service.promote();
      Assert.assertTrue(MetricsSystem.isStarted());
      service.demote();
      Assert.assertTrue(MetricsSystem.isStarted());
    }
    service.stop();
    Assert.assertFalse(MetricsSystem.isStarted());
  }

  @Test
  public void whenLeadingTest() {
    Configuration.set(PropertyKey.STANDBY_MASTER_METRICS_SINK_ENABLED, false);
    MetricsSimpleService service = MetricsSimpleService.Factory.create();
    Assert.assertTrue(service instanceof WhenLeadingMetricsSimpleService);

    Assert.assertFalse(MetricsSystem.isStarted());
    service.start();
    Assert.assertFalse(MetricsSystem.isStarted());
    for (int i = 0; i < 5; i++) {
      service.promote();
      Assert.assertTrue(MetricsSystem.isStarted());
      service.demote();
      Assert.assertFalse(MetricsSystem.isStarted());
    }
    service.stop();
    Assert.assertFalse(MetricsSystem.isStarted());
  }
}
