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

package alluxio.server.health;

import alluxio.master.LocalAlluxioCluster;
import alluxio.master.metrics.MetricsMaster;
import alluxio.metrics.Metric;
import alluxio.testutils.LocalAlluxioClusterResource;

import alluxio.util.executor.ExecutorServiceFactories;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class MetricMasterIntegrityIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder().build();
  private LocalAlluxioCluster mCluster;
  private MetricsMaster mMetricsMaster;
  private ExecutorService mExecutor;
  private int mNumWorkers = 3000;

  @Before
  public void before() {
    mCluster = mClusterResource.get();
    mMetricsMaster = mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(MetricsMaster.class);
    mExecutor = ExecutorServiceFactories.fixedThreadPool("alluxio-test", 50).create();
  }

  @Test
  public void testSameWorkerManyHeartbeats() throws Exception {
    Map<String, List<Metric>> map = mMetricsMaster.getWorkerMetrics();
    String hostname = "10.0.0.240";
    List<Metric> metrics = map.get(hostname);
    List<Future<?>> futureList = new ArrayList<>();
    long start = System.currentTimeMillis();
    for (int i = 0; i < 200; i++) {
      Future<?> future = mExecutor.submit(() -> mMetricsMaster.putWorkerMetric(hostname, metrics));
      futureList.add(future);
    }
    long mid = System.currentTimeMillis();
    System.out.println("submit task takes " + (mid - start));
    for (Future<?> future : futureList) {
      future.get();
    }
    System.out.println("get result takes " + (System.currentTimeMillis() - mid));
  }

  @Test
  public void testMultipleWorkersOneHeartbeat() throws Exception {
    // The real worker submitted metrics
    List<Metric> metrics = mMetricsMaster.getWorkerMetrics().get("10.0.0.240");

    Map<String, List<Metric>> multipleWorkerMetrics = generateMultipleWorkerMetrics(metrics, mNumWorkers);
    System.out.println(multipleWorkerMetrics.size());
    List<Future<?>> futures = new ArrayList<>();
    long start = System.currentTimeMillis();
    for (Map.Entry<String, List<Metric>> entry : multipleWorkerMetrics.entrySet()) {
      Future<?> future = mExecutor.submit(() -> mMetricsMaster.putWorkerMetric(entry.getKey(), entry.getValue()));
      futures.add(future);
    }

    long mid = System.currentTimeMillis();
    System.out.println("submit tasks takes " + (mid - start));
    System.out.println(futures.size());
    for (Future<?> future : futures) {
      future.get();
    }
    System.out.println("get result takes " + (System.currentTimeMillis() - mid));
  }

  private Map<String, List<Metric>> generateMultipleWorkerMetrics(List<Metric> metrics, int number) {
    Map<String, List<Metric>> result = new HashMap<>();
    for (int i = 0; i < number; i++) {
      String newHostname = "10_0_0_" + i;
      List<Metric> newMetricList = new ArrayList<>();
      for (Metric metric : metrics) {
        Metric newMetric = new Metric(metric.getInstanceType(), newHostname, metric.getMetricType(), metric.getName(), metric.getValue());
        if (metric.getTags().size() != 0) {
          for (Map.Entry<String, String> tagEntry : metric.getTags().entrySet()) {
            newMetric.addTag(tagEntry.getKey(), tagEntry.getValue());
          }
        }
        newMetricList.add(newMetric);
      }
      result.put(newHostname, newMetricList);
    }
    return result;
  }
}
