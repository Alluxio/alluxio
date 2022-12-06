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

package alluxio.master.service.jvmmonitor;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.service.NoopService;
import alluxio.master.service.SimpleService;
import alluxio.metrics.MetricsSystem;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for Jvm pause monitor simple service.
 */
public class JvmMonitorServiceTest {
  @Before
  public void before() {
    MetricsSystem.startSinks(Configuration.getString(PropertyKey.METRICS_CONF_FILE));
  }

  @After
  public void after() {
    MetricsSystem.clearAllMetrics();
    MetricsSystem.stopSinks();
  }

  @Test
  public void disabledServiceTest() {
    Configuration.set(PropertyKey.MASTER_JVM_MONITOR_ENABLED, false);
    SimpleService service = JvmMonitorService.Factory.create();
    Assert.assertTrue(service instanceof NoopService);
  }

  @Test
  public void enabledServiceTest() {
    Configuration.set(PropertyKey.MASTER_JVM_MONITOR_ENABLED, true);
    SimpleService service = JvmMonitorService.Factory.create();
    Assert.assertTrue(service instanceof JvmMonitorService);

    checkMetrics(0);
    service.start();
    checkMetrics(3);
    for (int i = 0; i < 5; i++) {
      service.promote();
      checkMetrics(3);
      service.demote();
      checkMetrics(3);
    }
    service.stop();
    checkMetrics(3);
  }

  @Test
  public void doubleStart() {
    Configuration.set(PropertyKey.MASTER_JVM_MONITOR_ENABLED, true);
    SimpleService service = JvmMonitorService.Factory.create();
    Assert.assertTrue(service instanceof JvmMonitorService);

    service.start();
    Assert.assertThrows("JVM pause monitor must not already exist",
        IllegalStateException.class, service::start);
  }

  private void checkMetrics(int expected) {
    // the jvm monitor metrics are under the
    long count =
        MetricsSystem.allMetrics().entrySet().stream().filter(entry -> entry.getKey().contains(
            "Server.")).count();
    Assert.assertEquals(expected, count);
  }
}
