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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Abstract class that provides a common method for parsing metrics data.
 */
public abstract class WebInterfaceAbstractMetricsServlet extends HttpServlet {

  protected ObjectMapper mObjectMapper;

  /**
   * Creates a new instance of {@link WebInterfaceAbstractMetricsServlet}.
   */
  public WebInterfaceAbstractMetricsServlet() {
    mObjectMapper = new ObjectMapper().registerModule(new MetricsModule(TimeUnit.SECONDS,
            TimeUnit.MILLISECONDS, false));
  }

  /**
   * Populates operation metrics for displaying in the UI
   *
   * @param request The {@link HttpServletRequest} object
   */
  protected void populateCounterValues(Map<String, Metric> operations,
      Map<String, Counter> rpcInvocations, HttpServletRequest request) {

    for (Map.Entry<String, Metric> entry : operations.entrySet()) {
      if (entry.getValue() instanceof Gauge) {
        request.setAttribute(entry.getKey(), ((Gauge) entry.getValue()).getValue());
      } else if (entry.getValue() instanceof Counter) {
        request.setAttribute(entry.getKey(), ((Counter) entry.getValue()).getCount());
      }
    }

    for (Map.Entry<String, Counter> entry : rpcInvocations.entrySet()) {
      request.setAttribute(entry.getKey(), entry.getValue().getCount());
    }

  }
}
