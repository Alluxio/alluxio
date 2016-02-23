/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.web;

import alluxio.master.MasterContext;
import alluxio.master.MasterSource;
import alluxio.metrics.MetricsSystem;
import alluxio.util.CommonUtils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;

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
  private final transient MetricsSystem mMasterMetricsSystem;

  /**
   * Creates a new instance of {@link WebInterfaceMasterMetricsServlet}.
   *
   * @param masterMetricsSystem Tachyon master metric system
   */
  public WebInterfaceMasterMetricsServlet(MetricsSystem masterMetricsSystem) {
    super();
    mMasterMetricsSystem = Preconditions.checkNotNull(masterMetricsSystem);
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
   * @throws IOException if an I/O error occurs
   */
  private void populateValues(HttpServletRequest request) throws IOException {
    MetricRegistry mr = mMasterMetricsSystem.getMetricRegistry();

    MasterSource masterSource = MasterContext.getMasterSource();

    Long masterCapacityTotal = (Long) mr.getGauges()
        .get(CommonUtils.argsToString(".", masterSource.getName(), MasterSource.CAPACITY_TOTAL))
        .getValue();
    Long masterCapacityUsed = (Long) mr.getGauges()
        .get(CommonUtils.argsToString(".", masterSource.getName(), MasterSource.CAPACITY_USED))
        .getValue();

    int masterCapacityUsedPercentage =
        (masterCapacityTotal > 0) ? (int) (100L * masterCapacityUsed / masterCapacityTotal) : 0;
    request.setAttribute("masterCapacityUsedPercentage", masterCapacityUsedPercentage);
    request.setAttribute("masterCapacityFreePercentage", 100 - masterCapacityUsedPercentage);

    Long masterUnderfsCapacityTotal = (Long) mr.getGauges().get(
        CommonUtils.argsToString(".", masterSource.getName(), MasterSource.UFS_CAPACITY_TOTAL))
        .getValue();
    Long masterUnderfsCapacityUsed = (Long) mr.getGauges().get(
        CommonUtils.argsToString(".", masterSource.getName(), MasterSource.UFS_CAPACITY_USED))
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

    Map<String, Metric> operations = new TreeMap<String, Metric>();
    operations.putAll(counters);
    String filesPinnedProperty =
        CommonUtils.argsToString(".", masterSource.getName(), MasterSource.FILES_PINNED);
    operations.put(filesPinnedProperty, mr.getGauges().get(filesPinnedProperty));

    populateCounterValues(operations, rpcInvocations, request);
  }
}
