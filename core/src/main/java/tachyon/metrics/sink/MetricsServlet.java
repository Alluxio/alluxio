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

package tachyon.metrics.sink;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;


public class MetricsServlet implements Sink {
  private static final String SERVLET_KEY_PATH = "path";

  private Properties mProperties;
  private MetricRegistry mMetricsRegistry;
  private ObjectMapper mObjectMapper;

  /**
   * Creates a MetricsServlet with a Properties and MetricRegistry.
   *
   * @param properties the properties which may contain path property.
   * @param registry the metric registry to register.
   */
  public MetricsServlet(Properties properties, MetricRegistry registry) {
    mProperties = properties;
    mMetricsRegistry = registry;
    mObjectMapper =
        new ObjectMapper().registerModule(new MetricsModule(TimeUnit.SECONDS,
            TimeUnit.MILLISECONDS, false));
  }

  private HttpServlet createServlet() {
    return new HttpServlet() {
      @Override
      protected void doGet(HttpServletRequest request, HttpServletResponse response)
          throws ServletException, IOException {
        response.setContentType(String.format("text/json;charset=utf-8"));
        response.setStatus(HttpServletResponse.SC_OK);
        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
        String result = mObjectMapper.writeValueAsString(mMetricsRegistry);
        response.getWriter().println(result);
      }
    };
  }

  /***
   * Get the ServletContextHandler of the metrics servlet.
   *
   * @return the ServletContextHandler object.
   */
  public ServletContextHandler getHandler() {
    ServletContextHandler contextHandler = new ServletContextHandler();
    String servletPath = mProperties.getProperty(SERVLET_KEY_PATH);
    contextHandler.setContextPath(servletPath);
    contextHandler.addServlet(new ServletHolder(createServlet()), "/");
    return contextHandler;
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void report() {}
}
