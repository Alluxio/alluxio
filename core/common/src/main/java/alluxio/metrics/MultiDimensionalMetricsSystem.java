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
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;

import io.prometheus.metrics.config.PrometheusProperties;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.core.metrics.Histogram;
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
import java.util.function.DoubleSupplier;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * A metrics system for all processes to define and collect all the metrics,
 * and expose all the metrics to the web server.
 */
public final class MultiDimensionalMetricsSystem {
  public static final Histogram DATA_ACCESS = Histogram.builder()
      .name("alluxio_data_access")
      .help("aggregated throughput of all the data access")
      .unit(Unit.BYTES)
      .labelNames("method")
      .build();

  public static final Counter META_OPERATION = Counter.builder()
      .name("alluxio_meta_operation")
      .help("counter of rpc calls of the meta operations")
      .labelNames("op")
      .build();

  public static final Counter UFS_DATA_ACCESS = Counter.builder()
      .name("alluxio_ufs_data_access")
      .help("amount of the ufs access")
      .unit(Unit.BYTES)
      .labelNames("method")
      .build();

  public static final Counter CACHED_DATA_READ = Counter.builder()
      .name("alluxio_cached_data_read")
      .help("amount of the read cached data")
      .unit(Unit.BYTES)
      .build();

  public static final Counter EXTERNAL_DATA_READ = Counter.builder()
      .name("alluxio_external_data_read")
      .help("amount of the read data when cache missed on client")
      .unit(Unit.BYTES)
      .build();

  public static final Counter CACHED_EVICTED_DATA = Counter.builder()
      .name("alluxio_cached_evicted_data")
      .help("amount of the evicted data")
      .unit(Unit.BYTES)
      .build();

  public static final DoubleSupplier NULL_SUPPLIER = () -> 0;
  private static DoubleSupplier sCacheStorageSupplier = NULL_SUPPLIER;
  private static DoubleSupplier sCacheManagerLoadingPagesSupplier = NULL_SUPPLIER;

  public static final GaugeWithCallback CACHED_STORAGE = GaugeWithCallback.builder()
      .name("alluxio_cached_storage")
      .help("amount of the cached data")
      .unit(Unit.BYTES)
      .callback(callback -> callback.call(sCacheStorageSupplier.getAsDouble()))
      .build();

  public static final GaugeWithCallback CACHED_CAPACITY = GaugeWithCallback.builder()
      .name("alluxio_cached_capacity")
      .help("configured maximum cache storage")
      .unit(Unit.BYTES)
      .callback(callback -> {
        List<String> sizes = Configuration.global().getList(PropertyKey.WORKER_PAGE_STORE_SIZES);
        long sum = sizes.stream().map(FormatUtils::parseSpaceSize).reduce(0L, Long::sum);
        callback.call(sum);
      })
      .build();
  public static final GaugeWithCallback CACHE_MANAGER_LOADING_PAGES = GaugeWithCallback.builder()
      .name("alluxio_cached_manager_loading_pages")
      .help("number of pages being loaded from UFS into the cache manager")
      .unit(Unit.BYTES)
      .callback(callback -> callback.call(sCacheManagerLoadingPagesSupplier.getAsDouble()))
      .build();

  // Distributed load related
  public static final Counter DISTRIBUTED_LOAD_JOB_FAILURE = Counter.builder()
      .name("alluxio_distributed_load_job_failure")
      .help("counter of the distributed load failure received on master")
      .labelNames("reason", "final_attempt", "worker")
      .register();
  public static final Counter DISTRIBUTED_LOAD_JOB_SCANNED = Counter.builder()
      .name("alluxio_distributed_load_job_scanned")
      .help("counter of the inodes scanned in distributed load")
      .register();
  public static final Counter DISTRIBUTED_LOAD_JOB_PROCESSED = Counter.builder()
      .name("alluxio_distributed_load_job_processed")
      .help("counter of the non empty file copies loaded in distributed load")
      .register();
  public static final Counter DISTRIBUTED_LOAD_JOB_SKIPPED = Counter.builder()
      .name("alluxio_distributed_load_job_skipped")
      .help("counter of the inodes skipped in distributed load")
      .register();
  public static final Counter DISTRIBUTED_LOAD_JOB_LOADED_BYTES = Counter.builder()
      .name("alluxio_distributed_load_job_loaded_bytes")
      .help("counter of the bytes loaded in distributed load")
      .register();
  public static final Counter DISTRIBUTED_LOAD_JOB_DISPATCHED_SIZE = Counter.builder()
      .name("alluxio_distributed_load_job_dispatched_size")
      .help("distributed_load_job_batch_size")
      .labelNames("worker")
      .register();
  public static final Counter DISTRIBUTED_LOAD_WORKER_SUBTASKS_TOTAL = Counter.builder()
      .name("alluxio_distributed_worker_subtasks_total")
      .help("counter of the total subtasks a worker processes")
      .register();
  public static final Counter DISTRIBUTED_LOAD_WORKER_BYTES_LOADED_TOTAL = Counter.builder()
      .name("alluxio_distributed_worker_bytes_loaded_total")
      .unit(Unit.BYTES)
      .help("counter of the total bytes loaded in distributed load of a worker")
      .register();
  public static final Counter DISTRIBUTED_LOAD_WORKER_ERRORS_TOTAL = Counter.builder()
      .name("alluxio_distributed_worker_errors_total")
      .help("counter of the total errors in distributed load of a worker")
      .register();

  // Client fallback related
  public static final Counter CLIENT_RPC_RETRY_ON_DIFFERENT_WORKERS = Counter.builder()
      .name("client_rpc_retry_on_different_workers")
      .help("counter of client retry on different workers if multi replica is enabled")
      .labelNames("op", "retry_count")
      .build();

  /**
   * Initialize all the metrics.
   */
  public static void initMetrics() {
    JvmMetrics.builder().register();
    if (CommonUtils.PROCESS_TYPE.get() != CommonUtils.ProcessType.WORKER
        && CommonUtils.PROCESS_TYPE.get() != CommonUtils.ProcessType.CLIENT) {
      return;
    }
    if (CommonUtils.PROCESS_TYPE.get() == CommonUtils.ProcessType.CLIENT) {
      PrometheusRegistry.defaultRegistry.register(EXTERNAL_DATA_READ);
    }
    PrometheusRegistry.defaultRegistry.register(DATA_ACCESS);
    PrometheusRegistry.defaultRegistry.register(UFS_DATA_ACCESS);
    PrometheusRegistry.defaultRegistry.register(META_OPERATION);
    PrometheusRegistry.defaultRegistry.register(CACHED_DATA_READ);
    PrometheusRegistry.defaultRegistry.register(CACHED_EVICTED_DATA);
    PrometheusRegistry.defaultRegistry.register(CACHED_STORAGE);
    PrometheusRegistry.defaultRegistry.register(CACHED_CAPACITY);
    PrometheusRegistry.defaultRegistry.register(CACHE_MANAGER_LOADING_PAGES);
  }

  /**
   * Set the supplier for CACHE_STORAGE metrics.
   *
   * @param supplier supplier for cache storage
   */
  public static void setCacheStorageSupplier(DoubleSupplier supplier) {
    sCacheStorageSupplier = supplier;
  }

  /**
   * @param supplier the supplier for CACHE_MANAGER_LOADING_PAGES
   */
  public static void setCacheManagerLoadingPagesSupplier(
      DoubleSupplier supplier) {
    MultiDimensionalMetricsSystem.sCacheManagerLoadingPagesSupplier = supplier;
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
