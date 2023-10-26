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

package alluxio.metrics;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.util.FormatUtils;

import io.prometheus.metrics.config.PrometheusProperties;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.core.metrics.Summary;
import io.prometheus.metrics.exporter.common.PrometheusHttpRequest;
import io.prometheus.metrics.exporter.common.PrometheusHttpResponse;
import io.prometheus.metrics.exporter.common.PrometheusScrapeHandler;
import io.prometheus.metrics.exporter.servlet.jakarta.HttpExchangeAdapter;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.Unit;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.List;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * A metrics system for all processes to define and collect all the metrics,
 * and expose all the metrics to the web server.
 */
public final class MultiDimensionalMetricsSystem {
  public static final Summary DATA_ACCESS = Summary.builder()
      .name("alluxio_data_access")
      .help("aggregated latency of all the data access")
      .unit(Unit.SECONDS)
      .labelNames("method")
      .register();

  public static final Summary UFS_DATA_ACCESS = Summary.builder()
      .name("alluxio_ufs_data_access")
      .help("aggregated latency of ufs access")
      .unit(Unit.SECONDS)
      .labelNames("method")
      .register();

  public static final Counter CACHED_DATA_READ = Counter.builder()
      .name("alluxio_cached_data_read")
      .help("amount of the read cached data")
      .unit(Unit.BYTES)
      .register();

  public static final Counter META_OPERATION = Counter.builder()
      .name("alluxio_meta_operation")
      .help("counter of rpc calls of the meta operations")
      .labelNames("op")
      .register();

  public static final Gauge CACHED_STORAGE = Gauge.builder()
      .name("alluxio_cached_storage")
      .help("amount of the cached data")
      .unit(Unit.BYTES)
      .register();

  public static final GaugeWithCallback CACHED_CAPACITY = GaugeWithCallback.builder()
      .name("alluxio_cached_capacity")
      .help("configured maximum cache storage")
      .unit(Unit.BYTES)
      .callback(callback -> {
        List<String> sizes = Configuration.global().getList(PropertyKey.WORKER_PAGE_STORE_SIZES);
        long sum = sizes.stream().map(FormatUtils::parseSpaceSize).reduce(0L, Long::sum);
        callback.call(sum);
      })
      .register();

  public static final Counter CACHED_EVICTED_DATA = Counter.builder()
      .name("alluxio_cached_evicted_data")
      .help("amount of the evicted data")
      .unit(Unit.BYTES)
      .register();

  public static final Gauge CACHED_READABLE_STORAGE = Gauge.builder()
      .name("alluxio_cached_readable_storage")
      .help("amount of readable cached data")
      .unit(Unit.BYTES)
      .register();

  /**
   * Initialize all the metrics.
   */
  public static void initMetrics() {
    JvmMetrics.builder().register();
  }

  /**
   * A servlet that exposes metrics data in prometheus format by HTTP.
   */
  public static class WebHandler extends HttpServlet {
    private static final long serialVersionUID = 1;
    private static final String SERVLET_PATH = "/metrics";

    private final PrometheusScrapeHandler mHandler = new PrometheusScrapeHandler(
        PrometheusProperties.get(), PrometheusRegistry.defaultRegistry);

    /**
     * Returns the servlet handler for Prometheus exposure.
     *
     * @return servlet handler
     */
    public static ServletContextHandler getHandler() {
      ServletContextHandler contextHandler = new ServletContextHandler();
      contextHandler.setContextPath(SERVLET_PATH);
      contextHandler.addServlet(new ServletHolder(new WebHandler()), "/");
      return contextHandler;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      mHandler.handleRequest(new ServletAdapter(req, resp));
    }
  }

  /**
   * An adapter to make the javax.servlet and jakarta.servlet compatible.
   */
  private static class ServletAdapter extends HttpExchangeAdapter {
    private final Request mRequest;
    private final Response mResponse;

    public ServletAdapter(HttpServletRequest request, HttpServletResponse response) {
      super(null, null);
      mRequest = new Request(request);
      mResponse = new Response(response);
    }

    @Override
    public PrometheusHttpRequest getRequest() {
      return mRequest;
    }

    @Override
    public PrometheusHttpResponse getResponse() {
      return mResponse;
    }

    @Override
    public void handleException(IOException e) throws IOException {
      throw e;
    }

    @Override
    public void handleException(RuntimeException e) {
      throw e;
    }

    @Override
    public void close() {}

    private static class Request implements PrometheusHttpRequest {
      private final HttpServletRequest mRequest;

      public Request(HttpServletRequest request) {
        mRequest = request;
      }

      @Override
      public String getQueryString() {
        return mRequest.getQueryString();
      }

      @Override
      public Enumeration<String> getHeaders(String name) {
        return mRequest.getHeaders(name);
      }

      @Override
      public String getMethod() {
        return mRequest.getMethod();
      }
    }

    private static class Response implements PrometheusHttpResponse {
      private final HttpServletResponse mResponse;

      public Response(HttpServletResponse response) {
        mResponse = response;
      }

      @Override
      public void setHeader(String name, String value) {
        mResponse.setHeader(name, value);
      }

      @Override
      public OutputStream sendHeadersAndGetBody(
          int statusCode, int contentLength) throws IOException {
        if (mResponse.getHeader("Content-Length") == null && contentLength > 0) {
          mResponse.setContentLength(contentLength);
        }

        mResponse.setStatus(statusCode);
        return mResponse.getOutputStream();
      }
    }
  }
}
