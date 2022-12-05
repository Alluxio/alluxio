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

import alluxio.Constants;
import alluxio.StreamCache;
import alluxio.client.file.FileSystem;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.audit.AsyncUserAccessAuditLogWriter;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proxy.ProxyProcess;
import alluxio.proxy.s3.CompleteMultipartUploadHandler;
import alluxio.proxy.s3.S3RequestServlet;
import alluxio.proxy.s3.S3RestExceptionMapper;
import alluxio.util.io.PathUtils;

import com.google.common.base.Stopwatch;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * The Alluxio proxy web server.
 */
@NotThreadSafe
public final class ProxyWebServer extends WebServer {
  private static final Logger LOG = LoggerFactory.getLogger(ProxyWebServer.class);
  public static final String ALLUXIO_PROXY_SERVLET_RESOURCE_KEY = "Alluxio Proxy";
  public static final String FILE_SYSTEM_SERVLET_RESOURCE_KEY = "File System";
  public static final String STREAM_CACHE_SERVLET_RESOURCE_KEY = "Stream Cache";

  public static final String SERVER_CONFIGURATION_RESOURCE_KEY = "Server Configuration";
  public static final String ALLUXIO_PROXY_AUDIT_LOG_WRITER_KEY = "Alluxio Proxy Audit Log Writer";

  public static FileSystem mFileSystem = null;

  public static AsyncUserAccessAuditLogWriter mAsyncAuditLogWriter;

  /**
   * Creates a new instance of {@link ProxyWebServer}.
   *
   * @param serviceName the service name
   * @param address the service address
   * @param proxyProcess the Alluxio proxy process
   */
  public ProxyWebServer(String serviceName, InetSocketAddress address,
      final ProxyProcess proxyProcess) {
    super(serviceName, address);

    // REST configuration
    ResourceConfig config = new ResourceConfig().packages("alluxio.proxy", "alluxio.proxy.s3",
            "alluxio.proxy.s3.logging")
        .register(JacksonProtobufObjectMapperProvider.class)
        .register(S3RestExceptionMapper.class);

//    ResourceConfig config = new ResourceConfig()
//            .register(S3AuthenticationFilter.class)
//            .register(JacksonProtobufObjectMapperProvider.class)
//            .register(S3RestExceptionMapper.class);;

    ProxyWebServer.mFileSystem = FileSystem.Factory.create(Configuration.global());

    if (Configuration.getBoolean(PropertyKey.PROXY_AUDIT_LOGGING_ENABLED)) {
      mAsyncAuditLogWriter = new AsyncUserAccessAuditLogWriter("PROXY_AUDIT_LOG");
      mAsyncAuditLogWriter.start();
      MetricsSystem.registerGaugeIfAbsent(
          MetricKey.PROXY_AUDIT_LOG_ENTRIES_SIZE.getName(),
              () -> mAsyncAuditLogWriter != null
                  ? mAsyncAuditLogWriter.getAuditLogEntriesSize() : -1);
    }

    ServletContainer servlet = new ServletContainer(config) {
      private static final long serialVersionUID = 7756010860672831556L;

      @Override
      public void init() throws ServletException {
        super.init();
        getServletContext().setAttribute(ALLUXIO_PROXY_SERVLET_RESOURCE_KEY, proxyProcess);
        getServletContext()
                .setAttribute(FILE_SYSTEM_SERVLET_RESOURCE_KEY, mFileSystem);
        getServletContext().setAttribute(STREAM_CACHE_SERVLET_RESOURCE_KEY,
                new StreamCache(Configuration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS)));
        getServletContext().setAttribute(ALLUXIO_PROXY_AUDIT_LOG_WRITER_KEY, mAsyncAuditLogWriter);
      }

      @Override
      public void service(final ServletRequest req, final ServletResponse res)
              throws ServletException, IOException {
        Stopwatch stopWatch = Stopwatch.createStarted();
        super.service(req, res);
        if ((req instanceof HttpServletRequest) && (res instanceof HttpServletResponse)) {
          HttpServletRequest httpReq = (HttpServletRequest) req;
          HttpServletResponse httpRes = (HttpServletResponse) res;

          OutputStream os = httpRes.getOutputStream();
          boolean isHandled = false;
          if (req instanceof org.eclipse.jetty.server.Request) {
            isHandled = ((Request) req).isHandled();
          }
          logAccess(httpReq, httpRes, stopWatch);
        }
      }

      @Override
      public void doFilter(final HttpServletRequest request, final HttpServletResponse response,
                           final FilterChain chain)
      {
        LOG.info("doFilter called");
      }

    };
//    ServletHolder servletHolder = new ServletHolder("Alluxio Proxy Web Service", servlet);
    ServletHolder servletHolder = new ServletHolder("Alluxio Proxy Web Service", S3RequestServlet.getInstance());
    mServletContextHandler
        .addServlet(servletHolder, PathUtils.concatPath(Constants.REST_API_PREFIX, "*"));
    // TODO(czhu): Move S3 API logging out of CompleteMultipartUploadHandler into a logging handler
    addHandler(new CompleteMultipartUploadHandler(mFileSystem, Constants.REST_API_PREFIX));
//    addHandler(S3RequstHandler.getInstance());
  }

  @Override
  public void stop() throws Exception {
    if (mAsyncAuditLogWriter != null) {
      mAsyncAuditLogWriter.stop();
      mAsyncAuditLogWriter = null;
    }
    mFileSystem.close();
    super.stop();
  }

  /**
   * Log the access of every single http request.
   * @param request
   * @param response
   * @param stopWatch
   */
  public static void logAccess(HttpServletRequest request, HttpServletResponse response,
                               Stopwatch stopWatch) {
    String contentLenStr = "None";
    if (request.getHeader("x-amz-decoded-content-length") != null) {
      contentLenStr = request.getHeader("x-amz-decoded-content-length");
    } else if (request.getHeader("Content-Length") != null) {
      contentLenStr = request.getHeader("Content-Length");
    }
    String accessLog = String.format("[ACCESSLOG] Request:%s - Status:%d "
                    + "- ContentLength:%s - Elapsed(ms):%d",
            request, response.getStatus(),
            contentLenStr, stopWatch.elapsed(TimeUnit.MILLISECONDS));
    if (LOG.isDebugEnabled()) {
      String requestHeaders = Collections.list(request.getHeaderNames()).stream()
              .map(x -> x + ":" + request.getHeader(x))
              .collect(Collectors.joining("\n"));
      String responseHeaders = response.getHeaderNames().stream()
              .map(x -> x + ":" + response.getHeader(x))
              .collect(Collectors.joining("\n"));
      String moreInfoStr = String.format("%n[RequestHeader]:%n%s%n[ResponseHeader]:%n%s",
              requestHeaders, responseHeaders);
      LOG.debug(accessLog + " " + moreInfoStr);
    } else {
      LOG.info(accessLog);
    }
  }
}
