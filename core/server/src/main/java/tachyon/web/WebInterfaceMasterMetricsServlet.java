/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.web;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;

import tachyon.master.TachyonMaster;
import tachyon.metrics.TachyonMetricRegistry;

/**
 * Servlet that provides data for viewing the metrics values
 */
@ThreadSafe
public final class WebInterfaceMasterMetricsServlet extends WebInterfaceAbstractMetricsServlet {

  private static final long serialVersionUID = -1481253168100363787L;
  private final transient TachyonMaster mMaster;

  /**
   * Creates a new instance of {@link WebInterfaceMasterMetricsServlet}.
   *
   * @param master Tachyon master
   */
  public WebInterfaceMasterMetricsServlet(TachyonMaster master) {
    super();
    mMaster = master;
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
    TachyonMetricRegistry mr = mMaster.getMasterMetricsSystem().getMetricRegistry();

    Long masterCapacityTotal = (Long) mr.getGauges().get("master.CapacityTotal").getValue();
    Long masterCapacityUsed = (Long) mr.getGauges().get("master.CapacityUsed").getValue();

    int masterCapacityUsedPercentage = (int) (100L * masterCapacityUsed / masterCapacityTotal);
    request.setAttribute("masterCapacityUsedPercentage", masterCapacityUsedPercentage);
    request.setAttribute("masterCapacityFreePercentage", 100 - masterCapacityUsedPercentage);

    Long masterUnderfsCapacityTotal =
        (Long) mr.getGauges().get("master.UnderFsCapacityTotal").getValue();
    Long masterUnderfsCapacityUsed =
        (Long) mr.getGauges().get("master.UnderFsCapacityUsed").getValue();

    int masterUnderfsCapacityUsedPercentage =
        (int) (100L * masterUnderfsCapacityUsed / masterUnderfsCapacityTotal);
    request.setAttribute("masterUnderfsCapacityUsedPercentage",
        masterUnderfsCapacityUsedPercentage);
    request.setAttribute("masterUnderfsCapacityFreePercentage",
        100 - masterUnderfsCapacityUsedPercentage);

    Map<String,Counter> counters = mr.getCounters(MetricFilter.ALL);

    Map<String,Metric> operations = new TreeMap<String, Metric>();
    operations.putAll(counters);
    operations.put("master.FilesPinned", mr.getGauges().get("master.FilesPinned"));

    populateCountersValues(mr,operations,request);
  }
}
