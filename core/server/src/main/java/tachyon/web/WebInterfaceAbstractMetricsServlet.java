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
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import tachyon.metrics.TachyonMetricRegistry;

/**
 * Abstract class that provides a common method for parsing metrics data
 */
public abstract class WebInterfaceAbstractMetricsServlet extends HttpServlet {

  protected ObjectMapper mObjectMapper;

  /**
   * create a new instance of {@link WebInterfaceAbstractMetricsServlet}.
   */
  public WebInterfaceAbstractMetricsServlet() {
    mObjectMapper = new ObjectMapper().registerModule(new MetricsModule(TimeUnit.SECONDS,
            TimeUnit.MILLISECONDS, false));
  }

  /**
   * Populates key, value pairs for UI display.
   *
   * @param mr the metric registry
   * @param request The {@link HttpServletRequest} object
   * @throws IOException if an I/O error occurs
   */
  protected void populateCountersValues(TachyonMetricRegistry mr, Map<String, Metric> operations,
        Map<String, Counter> rpcInvocations, HttpServletRequest request) throws IOException {
    request.setAttribute("historyEnabled", mr.isHistoryEnabled());

    if (mr.isHistoryEnabled()) {
      request.setAttribute("csvPath", mr.getCsvPath());

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

      request.setAttribute("operationMetrics",
              mObjectMapper.writeValueAsString(countersHistorycal));
    } else {
      for (Map.Entry<String, Metric> entry : operations.entrySet()) {
        if (entry.getValue() instanceof Gauge) {
          request.setAttribute(entry.getKey(),((Gauge) entry.getValue()).getValue());
        } else if (entry.getValue() instanceof Counter) {
          request.setAttribute(entry.getKey(), ((Counter) entry.getValue()).getCount());
        }
      }
    }

    for (Map.Entry<String, Counter> entry : rpcInvocations.entrySet()) {
      request.setAttribute(entry.getKey(), entry.getValue().getCount());
    }

  }
}
