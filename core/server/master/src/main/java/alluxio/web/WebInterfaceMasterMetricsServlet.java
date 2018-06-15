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

package alluxio.web;

import alluxio.master.block.DefaultBlockMaster;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.metrics.ClientMetrics;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.WorkerMetrics;
import alluxio.util.FormatUtils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet that provides data for viewing the master metrics values.
 */
@ThreadSafe
public final class WebInterfaceMasterMetricsServlet extends WebInterfaceAbstractMetricsServlet {
  private static final long serialVersionUID = -1481253168100363787L;

  /**
   * Create a {@link WebInterfaceMasterMetricsServlet} instance.
   */
  public WebInterfaceMasterMetricsServlet() {
    super();
  }

  /**
   * Redirects the request to a JSP after populating attributes via populateValues.
   *
   * @param request the {@link HttpServletRequest} object
   * @param response the {@link HttpServletResponse} object
   * @throws ServletException if the target resource throws this exception
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    populateValues(request);
    getServletContext().getRequestDispatcher("/metrics.jsp").forward(request, response);
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    populateValues(request);
    getServletContext().getRequestDispatcher("/metrics.jsp").forward(request, response);
  }

  /**
   * Populates key, value pairs for UI display.
   *
   * @param request The {@link HttpServletRequest} object
   */
  private void populateValues(HttpServletRequest request) throws IOException {
    MetricRegistry mr = MetricsSystem.METRIC_REGISTRY;

    Long masterCapacityTotal = (Long) mr.getGauges()
        .get(MetricsSystem.getMetricName(DefaultBlockMaster.Metrics.CAPACITY_TOTAL))
        .getValue();
    Long masterCapacityUsed = (Long) mr.getGauges()
        .get(MetricsSystem.getMetricName(DefaultBlockMaster.Metrics.CAPACITY_USED))
        .getValue();

    int masterCapacityUsedPercentage =
        (masterCapacityTotal > 0) ? (int) (100L * masterCapacityUsed / masterCapacityTotal) : 0;
    request.setAttribute("masterCapacityUsedPercentage", masterCapacityUsedPercentage);
    request.setAttribute("masterCapacityFreePercentage", 100 - masterCapacityUsedPercentage);

    Long masterUnderfsCapacityTotal = (Long) mr.getGauges()
        .get(MetricsSystem.getMetricName(DefaultFileSystemMaster.Metrics.UFS_CAPACITY_TOTAL))
        .getValue();
    Long masterUnderfsCapacityUsed = (Long) mr.getGauges()
        .get(MetricsSystem.getMetricName(DefaultFileSystemMaster.Metrics.UFS_CAPACITY_USED))
        .getValue();

    int masterUnderfsCapacityUsedPercentage = (masterUnderfsCapacityTotal > 0)
        ? (int) (100L * masterUnderfsCapacityUsed / masterUnderfsCapacityTotal) : 0;
    request.setAttribute("masterUnderfsCapacityUsedPercentage",
        masterUnderfsCapacityUsedPercentage);
    request.setAttribute("masterUnderfsCapacityFreePercentage",
        100 - masterUnderfsCapacityUsedPercentage);

    populateClusterMetrics(request);

    Map<String, Counter> counters = mr.getCounters(new MetricFilter() {
      @Override
      public boolean matches(String name, Metric metric) {
        return !(name.endsWith("Ops"));
      }
    });

    Map<String, Counter> rpcInvocations = mr.getCounters(new MetricFilter() {
      @Override
      public boolean matches(String name, Metric metric) {
        return name.endsWith("Ops");
      }
    });

    Map<String, Metric> operations = new TreeMap<>();
    // Remove the instance name from the metrics.
    for (Map.Entry<String, Counter> entry : counters.entrySet()) {
      operations.put(MetricsSystem.stripInstanceAndHost(entry.getKey()), entry.getValue());
    }
    String filesPinnedProperty =
        MetricsSystem.getMetricName(DefaultFileSystemMaster.Metrics.FILES_PINNED);
    operations.put(MetricsSystem.stripInstanceAndHost(filesPinnedProperty),
        mr.getGauges().get(filesPinnedProperty));

    Map<String, Counter> rpcInvocationsUpdated = new TreeMap<>();
    for (Map.Entry<String, Counter> entry : rpcInvocations.entrySet()) {
      rpcInvocationsUpdated.put(MetricsSystem.stripInstanceAndHost(entry.getKey()),
          entry.getValue());
    }

    populateCounterValues(operations, rpcInvocationsUpdated, request);
  }

  private void populateClusterMetrics(HttpServletRequest request) throws IOException {
    MetricRegistry mr = MetricsSystem.METRIC_REGISTRY;

    // cluster read size
    Long bytesReadLocal = (Long) mr.getGauges()
        .get(MetricsSystem.getClusterMetricName(ClientMetrics.BYTES_READ_LOCAL)).getValue();
    Long bytesReadRemote = (Long) mr.getGauges()
        .get(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_ALLUXIO)).getValue();
    Long bytesReadUfs = (Long) mr.getGauges()
        .get(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_UFS_ALL)).getValue();
    request.setAttribute("totalBytesReadLocal", FormatUtils.getSizeFromBytes(bytesReadLocal));
    request.setAttribute("totalBytesReadRemote", FormatUtils.getSizeFromBytes(bytesReadRemote));
    request.setAttribute("totalBytesReadUfs", FormatUtils.getSizeFromBytes(bytesReadUfs));

    // cluster cache hit and miss
    long bytesReadTotal = bytesReadLocal + bytesReadRemote + bytesReadUfs;
    double cacheHitLocalPercentage =
        (bytesReadTotal > 0) ? (100D * bytesReadLocal / bytesReadTotal) : 0;
    double cacheHitRemotePercentage =
        (bytesReadTotal > 0) ? (100D * bytesReadRemote / bytesReadTotal) : 0;
    double cacheMissPercentage = (bytesReadTotal > 0) ? (100D * bytesReadUfs / bytesReadTotal) : 0;

    request.setAttribute("cacheHitLocal", String.format("%.2f", cacheHitLocalPercentage));
    request.setAttribute("cacheHitRemote", String.format("%.2f", cacheHitRemotePercentage));
    request.setAttribute("cacheMiss", String.format("%.2f", cacheMissPercentage));

    // cluster write size
    Long bytesWrittenAlluxio = (Long) mr.getGauges()
        .get(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_ALLUXIO)).getValue();
    Long bytesWrittenUfs = (Long) mr.getGauges()
        .get(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_UFS_ALL)).getValue();
    request.setAttribute("totalBytesWrittenAlluxio",
        FormatUtils.getSizeFromBytes(bytesWrittenAlluxio));
    request.setAttribute("totalBytesWrittenUfs", FormatUtils.getSizeFromBytes(bytesWrittenUfs));

    // cluster read throughput
    Long bytesReadLocalThroughput = (Long) mr.getGauges()
        .get(MetricsSystem.getClusterMetricName(ClientMetrics.BYTES_READ_LOCAL_THROUGHPUT))
        .getValue();
    Long bytesReadRemoteThroughput = (Long) mr.getGauges()
        .get(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_ALLUXIO_THROUGHPUT))
        .getValue();
    Long bytesReadUfsThroughput = (Long) mr.getGauges()
        .get(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_UFS_THROUGHPUT))
        .getValue();
    request.setAttribute("totalBytesReadLocalThroughput",
        FormatUtils.getSizeFromBytes(bytesReadLocalThroughput));
    request.setAttribute("totalBytesReadRemoteThroughput",
        FormatUtils.getSizeFromBytes(bytesReadRemoteThroughput));
    request.setAttribute("totalBytesReadUfsThroughput",
        FormatUtils.getSizeFromBytes(bytesReadUfsThroughput));

    // cluster write throughput
    Long bytesWrittenAlluxioThroughput = (Long) mr.getGauges()
        .get(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_ALLUXIO_THROUGHPUT))
        .getValue();
    Long bytesWrittenUfsThroughput = (Long) mr.getGauges()
        .get(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_UFS_THROUGHPUT))
        .getValue();
    request.setAttribute("totalBytesWrittenAlluxioThroughput",
        FormatUtils.getSizeFromBytes(bytesWrittenAlluxioThroughput));
    request.setAttribute("totalBytesWrittenUfsThroughput",
        FormatUtils.getSizeFromBytes(bytesWrittenUfsThroughput));

    // cluster per UFS read
    Map<String, String> ufsReadSizeMap = new TreeMap<>();
    for (Map.Entry<String, Gauge> entry : mr.getGauges(new MetricFilter() {
      @Override
      public boolean matches(String name, Metric metric) {
        return name.contains(WorkerMetrics.BYTES_READ_UFS);
      }
      }).entrySet()) {
      alluxio.metrics.Metric metric =
          alluxio.metrics.Metric.from(entry.getKey(), (long) entry.getValue().getValue());
      ufsReadSizeMap.put(metric.getTags().get(WorkerMetrics.TAG_UFS),
          FormatUtils.getSizeFromBytes((long) metric.getValue()));
    }
    request.setAttribute("ufsReadSize", ufsReadSizeMap);

    // cluster per UFS write
    Map<String, String> ufsWriteSizeMap = new TreeMap<>();
    for (Map.Entry<String, Gauge> entry : mr.getGauges(new MetricFilter() {
      @Override
      public boolean matches(String name, Metric metric) {
        return name.contains(WorkerMetrics.BYTES_WRITTEN_UFS);
      }
      }).entrySet()) {
      alluxio.metrics.Metric metric =
          alluxio.metrics.Metric.from(entry.getKey(), (long) entry.getValue().getValue());
      ufsWriteSizeMap.put(metric.getTags().get(WorkerMetrics.TAG_UFS),
          FormatUtils.getSizeFromBytes((long) metric.getValue()));
    }
    request.setAttribute("ufsWriteSize", ufsWriteSizeMap);

    // per UFS ops
    Map<String, Map<String, Long>> ufsOpsMap = new TreeMap<>();
    for (Map.Entry<String, Gauge> entry : mr.getGauges(new MetricFilter() {
      @Override
      public boolean matches(String name, Metric metric) {
        return name.contains(WorkerMetrics.UFS_OP_PREFIX);
      }
      }).entrySet()) {
      alluxio.metrics.Metric metric =
          alluxio.metrics.Metric.from(entry.getKey(), (long) entry.getValue().getValue());
      if (!metric.getTags().containsKey(WorkerMetrics.TAG_UFS)) {
        continue;
      }
      String ufs = metric.getTags().get(WorkerMetrics.TAG_UFS);
      Map<String, Long> perUfsMap = ufsOpsMap.getOrDefault(ufs, new TreeMap<>());
      perUfsMap.put(metric.getName().replaceFirst(WorkerMetrics.UFS_OP_PREFIX, ""),
          (long) metric.getValue());
      ufsOpsMap.put(ufs, perUfsMap);
    }
    request.setAttribute("ufsOps", ufsOpsMap);
  }
}
