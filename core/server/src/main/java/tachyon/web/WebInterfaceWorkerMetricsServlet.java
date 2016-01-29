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

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;

import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import tachyon.metrics.TachyonMetricRegistry;
import tachyon.worker.TachyonWorker;

/**
 * Servlet that provides data for viewing the metrics values
 */
public final class WebInterfaceWorkerMetricsServlet extends HttpServlet {

  private static final long serialVersionUID = -1481253168100363787L;
  private final transient TachyonWorker mWorker;
  private ObjectMapper mObjectMapper;

  /**
   * Creates a new instance of {@link WebInterfaceWorkerMetricsServlet}.
   *
   * @param worker Tachyon worker
   */
  public WebInterfaceWorkerMetricsServlet(TachyonWorker worker) {
    mObjectMapper = new ObjectMapper().registerModule(new MetricsModule(TimeUnit.SECONDS,
            TimeUnit.MILLISECONDS, false));
    mWorker = worker;
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
    TachyonMetricRegistry mr = mWorker.getWorkerMetricsSystem().getMetricRegistry();

    Long workerCapacityTotal = (Long) mr.getGauges().get("worker.CapacityTotal").getValue();
    Long workerCapacityUsed = (Long) mr.getGauges().get("worker.CapacityUsed").getValue();

    int workerCapacityUsedPercentage = (int) (100L * workerCapacityUsed / workerCapacityTotal);
    request.setAttribute("workerCapacityUsedPercentage", workerCapacityUsedPercentage);
    request.setAttribute("workerCapacityFreePercentage", 100 - workerCapacityUsedPercentage);

    request.setAttribute("historyEnabled", mr.isHistoryEnabled());

    request.setAttribute("csvPath", mr.getCsvPath());

    Map<String,Counter> counters = mr.getCounters(new MetricFilter() {
      @Override
      public boolean matches(String name, Metric metric) {
        return !name.endsWith("Ops");
      }
    });

    Map<String,Metric> operations = new TreeMap<String, Metric>();
    operations.putAll(counters);
    operations.put("worker.BlocksCached", mr.getGauges().get("worker.BlocksCached"));

    CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");

    Map<String,Map<String,String>> countersHistorycal = new HashMap<String, Map<String,String>>();

    for (String k : operations.keySet()) {
      Map<String,String> values = new HashMap<String, String>();
      String fileName = mr.getCsvPath() + "/" + k + ".csv";
      CSVParser csvFileParser = new CSVParser(new FileReader(fileName), csvFileFormat);
      for (CSVRecord r : csvFileParser.getRecords()) {
        values.put(r.get(0), r.get(1));
      }
      countersHistorycal.put(k, values);
    }

    request.setAttribute("operationMetrics", mObjectMapper.writeValueAsString(countersHistorycal));
  }
}
