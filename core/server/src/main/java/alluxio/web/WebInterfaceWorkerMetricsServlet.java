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

import alluxio.metrics.MetricsSystem;
import alluxio.util.CommonUtils;
import alluxio.worker.WorkerContext;
import alluxio.worker.WorkerSource;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet that provides data for viewing the worker metrics values.
 */
public final class WebInterfaceWorkerMetricsServlet extends WebInterfaceAbstractMetricsServlet {

  private static final long serialVersionUID = -1481253168100363787L;
  private final transient MetricsSystem mWorkerMetricsSystem;

  /**
   * Creates a new instance of {@link WebInterfaceWorkerMetricsServlet}.
   *
   * @param workerMetricsSystem Alluxio worker metrics system
   */
  public WebInterfaceWorkerMetricsServlet(MetricsSystem workerMetricsSystem) {
    super();
    mWorkerMetricsSystem = Preconditions.checkNotNull(workerMetricsSystem);
  }

  /**
   * Redirects the request to a JSP after populating attributes via populateValues.
   *
   * @param request the {@link HttpServletRequest} object
   * @param response the {@link HttpServletResponse} object
   * @throws ServletException if the target resource throws this exception
   * @throws IOException if the target resource throws this exception
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    populateValues(request);
    getServletContext().getRequestDispatcher("/worker/metrics.jsp").forward(request, response);
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    populateValues(request);
    getServletContext().getRequestDispatcher("/worker/metrics.jsp").forward(request, response);
  }

  /**
   * Populates key, value pairs for UI display.
   *
   * @param request The {@link HttpServletRequest} object
   * @throws IOException if an I/O error occurs
   */
  private void populateValues(HttpServletRequest request) throws IOException {
    MetricRegistry mr = mWorkerMetricsSystem.getMetricRegistry();

    WorkerSource workerSource = WorkerContext.getWorkerSource();

    Long workerCapacityTotal = (Long) mr.getGauges()
        .get(CommonUtils.argsToString(".", workerSource.getName(), WorkerSource.CAPACITY_TOTAL))
        .getValue();
    Long workerCapacityUsed = (Long) mr.getGauges()
        .get(CommonUtils.argsToString(".", workerSource.getName(), WorkerSource.CAPACITY_USED))
        .getValue();

    int workerCapacityUsedPercentage =
        (workerCapacityTotal > 0) ? (int) (100L * workerCapacityUsed / workerCapacityTotal) : 0;
    request.setAttribute("workerCapacityUsedPercentage", workerCapacityUsedPercentage);
    request.setAttribute("workerCapacityFreePercentage", 100 - workerCapacityUsedPercentage);

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
    for (Map.Entry<String, Counter> entry: counters.entrySet()) {
      operations.put(removeHostNameFromWorkerMetricsName(entry.getKey()), entry.getValue());
    }
    String blockCachedProperty =
        CommonUtils.argsToString(".", workerSource.getName(), WorkerSource.BLOCKS_CACHED);
    operations.put(removeHostNameFromWorkerMetricsName(blockCachedProperty),
        mr.getGauges().get(blockCachedProperty));

    Map<String, Counter> rpcInvocationsUpdated = new TreeMap<>();
    for (Map.Entry<String, Counter> entry : rpcInvocations.entrySet()) {
      rpcInvocationsUpdated
          .put(removeHostNameFromWorkerMetricsName(entry.getKey()), entry.getValue());
    }
    populateCounterValues(operations, rpcInvocationsUpdated, request);
  }

  /**
   * Remove the worker name from worker metrics name to make it easier to retrieve worker metrics
   * in the worker metrics dashboard.
   *
   * @param metricsName the metrics name with hostname builtin
   * @return the metrics with hostname removed
   */
  private String removeHostNameFromWorkerMetricsName(String metricsName) {
    String[] pieces = metricsName.split("\\.");
    if (pieces.length < 3) {
      throw new IllegalArgumentException("Incorrect worker metrics name: " + metricsName);
    }
    pieces[1] = null;
    return Joiner.on(".").skipNulls().join(pieces);
  }
}
