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

package alluxio.master.throttle;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.JvmPauseMonitor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * The indicators test.
 */
public class IndicatorsTests {
  private static final JvmPauseMonitor JVM_PAUSE_MONITOR = new JvmPauseMonitor(
      Configuration.getMs(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS),
      Configuration.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS),
      Configuration.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS));

  @Before
  public void before() {
    Configuration.set(PropertyKey.MASTER_JVM_MONITOR_ENABLED, true);
    JVM_PAUSE_MONITOR.start();
    MetricsSystem.registerGaugeIfAbsent(
        MetricsSystem.getMetricName(MetricKey.TOTAL_EXTRA_TIME.getName()),
        JVM_PAUSE_MONITOR::getTotalExtraTime);
    MetricsSystem.registerGaugeIfAbsent(
        MetricsSystem.getMetricName(MetricKey.INFO_TIME_EXCEEDED.getName()),
        JVM_PAUSE_MONITOR::getInfoTimeExceeded);
    MetricsSystem.registerGaugeIfAbsent(
        MetricsSystem.getMetricName(MetricKey.WARN_TIME_EXCEEDED.getName()),
        JVM_PAUSE_MONITOR::getWarnTimeExceeded);
  }

  @After
  public void after() {
    JVM_PAUSE_MONITOR.stop();
  }

  @Test
  public void basicIndicatorCreationTest() throws InterruptedException {
    int directMemSize = 101;
    ByteBuffer.allocateDirect(directMemSize);
    long baseSize = MetricsMonitorUtils.getDirectMemUsed();
    ByteBuffer.allocateDirect(directMemSize);
    ServerIndicator serverIndicator1
        = ServerIndicator.createFromMetrics(0);
    Thread.sleep(2000);
    Assert.assertEquals(directMemSize + baseSize, serverIndicator1.getDirectMemUsed());
    ServerIndicator serverIndicator2
        = ServerIndicator.createFromMetrics(0);
    Assert.assertEquals(directMemSize + baseSize, serverIndicator2.getDirectMemUsed());
    Assert.assertEquals(true, serverIndicator1.getPitTimeMS()
        < serverIndicator2.getPitTimeMS());
    Assert.assertEquals(true, serverIndicator1.getTotalJVMPauseTimeMS()
        <= serverIndicator2.getTotalJVMPauseTimeMS());
    Assert.assertEquals(serverIndicator1.getDirectMemUsed(), serverIndicator2.getDirectMemUsed());
    Assert.assertEquals(serverIndicator1.getHeapMax(), serverIndicator2.getHeapMax());
    Assert.assertEquals(serverIndicator1.getHeapUsed(), serverIndicator2.getHeapUsed());
    Assert.assertEquals(serverIndicator1.getRpcQueueSize(), serverIndicator2.getRpcQueueSize());
  }

  @Test
  public void basicIndicatorComparisonTest() throws InterruptedException {
    long directMemUsed = 8;
    long headMax = 1000;
    long headUsed = 800;
    double cpuLoad = 0.3;
    long totalJVMPauseTimeMS = 200;
    long pitTotalJVMPauseTime = 30;
    long rpcQueueSize = 50;
    long snapshotTimeMS = 33;
    ServerIndicator serverIndicator = new ServerIndicator(directMemUsed, headMax, headUsed, cpuLoad,
        totalJVMPauseTimeMS, pitTotalJVMPauseTime, rpcQueueSize, snapshotTimeMS);

    int times = 3;
    ServerIndicator serverIndicatorM = new ServerIndicator(serverIndicator, times);
    Assert.assertEquals(serverIndicatorM.getHeapUsed(),
        (serverIndicator.getHeapUsed() * times));
    Assert.assertEquals(serverIndicatorM.getCpuLoad(),
        (serverIndicator.getCpuLoad() * times), 0.0000001);
    Assert.assertEquals(serverIndicatorM.getDirectMemUsed(),
        (serverIndicator.getDirectMemUsed() * times));
    Assert.assertEquals(serverIndicatorM.getTotalJVMPauseTimeMS(),
        (serverIndicator.getTotalJVMPauseTimeMS() * times));
    Assert.assertEquals(serverIndicatorM.getRpcQueueSize(),
        (serverIndicator.getRpcQueueSize() * times));

    long baseDeltaLong = 4;
    double baseDeltaDouble = 0.1;
    long deltaLong = baseDeltaLong;
    double deltaDouble = baseDeltaDouble;
    ServerIndicator serverIndicator1Delta = new ServerIndicator(directMemUsed + deltaLong,
        headMax,
        headUsed + deltaLong, cpuLoad + deltaDouble,
        totalJVMPauseTimeMS + deltaLong, pitTotalJVMPauseTime + deltaLong,
        rpcQueueSize + deltaLong, snapshotTimeMS);

    ServerIndicator deltaIndicator = new ServerIndicator(serverIndicator1Delta);
    deltaIndicator.reduction(serverIndicator);
    Assert.assertEquals(deltaLong, deltaIndicator.getRpcQueueSize());
    Assert.assertEquals(deltaDouble, deltaIndicator.getCpuLoad(), 0.00001);
    Assert.assertEquals(deltaLong, deltaIndicator.getDirectMemUsed());
    Assert.assertEquals(deltaLong, deltaIndicator.getHeapUsed());
    Assert.assertEquals(deltaLong, deltaIndicator.getTotalJVMPauseTimeMS());

    deltaDouble += deltaDouble;
    deltaLong += deltaLong;
    ServerIndicator serverIndicator2Delta = new ServerIndicator(directMemUsed + deltaLong,
        headMax,
        headUsed + deltaLong, cpuLoad + deltaDouble,
        totalJVMPauseTimeMS + deltaLong, pitTotalJVMPauseTime + deltaLong,
        rpcQueueSize + deltaLong, snapshotTimeMS);
    deltaIndicator = new ServerIndicator(serverIndicator2Delta);
    deltaIndicator.reduction(serverIndicator);
    Assert.assertEquals(deltaIndicator.getRpcQueueSize(), deltaLong);
    Assert.assertEquals(deltaIndicator.getCpuLoad(), deltaDouble, 0.00001);
    Assert.assertEquals(deltaIndicator.getDirectMemUsed(), deltaLong);
    Assert.assertEquals(deltaIndicator.getHeapUsed(), deltaLong);
    Assert.assertEquals(deltaIndicator.getTotalJVMPauseTimeMS(), deltaLong);

    ServerIndicator serverAgg = new ServerIndicator(serverIndicator);
    serverAgg.addition(serverIndicator);
    serverAgg.addition(serverIndicator);
    serverAgg.reduction(serverIndicator);
    int aggTimes = 2;
    Assert.assertEquals(aggTimes * serverIndicator.getRpcQueueSize(), serverAgg.getRpcQueueSize());
    Assert.assertEquals(aggTimes * serverIndicator.getDirectMemUsed(), serverAgg.getDirectMemUsed());
    Assert.assertEquals(aggTimes * serverIndicator.getHeapUsed(), serverAgg.getHeapUsed());
    Assert.assertEquals(aggTimes * serverIndicator.getTotalJVMPauseTimeMS(),
        serverAgg.getTotalJVMPauseTimeMS());
    Assert.assertEquals(aggTimes * serverIndicator.getCpuLoad(),
        serverAgg.getCpuLoad(), 0.00001);
  }
}

