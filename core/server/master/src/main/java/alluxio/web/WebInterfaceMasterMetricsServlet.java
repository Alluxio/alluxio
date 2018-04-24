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
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;
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
        .get(MetricsSystem.getMasterMetricName(DefaultBlockMaster.Metrics.CAPACITY_TOTAL))
        .getValue();
    Long masterCapacityUsed = (Long) mr.getGauges()
        .get(MetricsSystem.getMasterMetricName(DefaultBlockMaster.Metrics.CAPACITY_USED))
        .getValue();

    int masterCapacityUsedPercentage =
        (masterCapacityTotal > 0) ? (int) (100L * masterCapacityUsed / masterCapacityTotal) : 0;
    request.setAttribute("masterCapacityUsedPercentage", masterCapacityUsedPercentage);
    request.setAttribute("masterCapacityFreePercentage", 100 - masterCapacityUsedPercentage);

    Long masterUnderfsCapacityTotal = (Long) mr.getGauges()
        .get(MetricsSystem.getMasterMetricName(DefaultFileSystemMaster.Metrics.UFS_CAPACITY_TOTAL))
        .getValue();
    Long masterUnderfsCapacityUsed = (Long) mr.getGauges()
        .get(MetricsSystem.getMasterMetricName(DefaultFileSystemMaster.Metrics.UFS_CAPACITY_USED))
        .getValue();

    int masterUnderfsCapacityUsedPercentage = (masterUnderfsCapacityTotal > 0)
        ? (int) (100L * masterUnderfsCapacityUsed / masterUnderfsCapacityTotal) : 0;
    request.setAttribute("masterUnderfsCapacityUsedPercentage",
        masterUnderfsCapacityUsedPercentage);
    request.setAttribute("masterUnderfsCapacityFreePercentage",
        100 - masterUnderfsCapacityUsedPercentage);

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
        MetricsSystem.getMasterMetricName(DefaultFileSystemMaster.Metrics.FILES_PINNED);
    operations.put(MetricsSystem.stripInstanceAndHost(filesPinnedProperty),
        mr.getGauges().get(filesPinnedProperty));

    Map<String, Counter> rpcInvocationsUpdated = new TreeMap<>();
    for (Map.Entry<String, Counter> entry : rpcInvocations.entrySet()) {
      rpcInvocationsUpdated
          .put(MetricsSystem.stripInstanceAndHost(entry.getKey()), entry.getValue());
    }

    populateCounterValues(operations, rpcInvocationsUpdated, request);
  }
}
