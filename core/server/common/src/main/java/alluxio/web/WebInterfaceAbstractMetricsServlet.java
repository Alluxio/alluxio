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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;

/**
 * Abstract class that provides a common method for parsing metrics data.
 */
public abstract class WebInterfaceAbstractMetricsServlet extends HttpServlet {
  private static final long serialVersionUID = -849266423481584779L;

  protected ObjectMapper mObjectMapper;

  /**
   * Creates a new instance of {@link WebInterfaceAbstractMetricsServlet}.
   */
  public WebInterfaceAbstractMetricsServlet() {
    mObjectMapper = new ObjectMapper().registerModule(new MetricsModule(TimeUnit.SECONDS,
            TimeUnit.MILLISECONDS, false));
  }

  /**
   * Populates operation metrics for displaying in the UI.
   *
   * @param request The {@link HttpServletRequest} object
   */
  protected void populateCounterValues(Map<String, Metric> operations,
      Map<String, Counter> rpcInvocations, HttpServletRequest request) {

    for (Map.Entry<String, Metric> entry : operations.entrySet()) {
      if (entry.getValue() instanceof Gauge) {
        request.setAttribute(entry.getKey(), ((Gauge<?>) entry.getValue()).getValue());
      } else if (entry.getValue() instanceof Counter) {
        request.setAttribute(entry.getKey(), ((Counter) entry.getValue()).getCount());
      }
    }

    for (Map.Entry<String, Counter> entry : rpcInvocations.entrySet()) {
      request.setAttribute(entry.getKey(), entry.getValue().getCount());
    }
  }
}
