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
import alluxio.proxy.s3.S3BaseTask;
import alluxio.proxy.s3.S3Handler;
import alluxio.proxy.s3.S3RequestServlet;
import alluxio.proxy.s3.S3RestExceptionMapper;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Stopwatch;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
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
  private final FileSystem mFileSystem;
  private AsyncUserAccessAuditLogWriter mAsyncAuditLogWriter;
  public static final String PROXY_S3_HANDLER_MAP = "Proxy S3 Handler Map";
  public ConcurrentHashMap<Request, S3Handler> mS3HandlerMap = new ConcurrentHashMap<>();

  class ProxyListener implements HttpChannel.Listener {
    public void onComplete(Request request)
    {
      S3Handler s3Hdlr = mS3HandlerMap.get(request);
      if (s3Hdlr != null) {
        ProxyWebServer.logAccess(s3Hdlr.getServletRequest(), s3Hdlr.getServletResponse(),
            s3Hdlr.getStopwatch(), s3Hdlr.getS3Task() != null
                ? s3Hdlr.getS3Task().getOPType() : S3BaseTask.OpType.Unknown);
      }
    }
  }

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
    String[] packages = {"alluxio.proxy", "alluxio.proxy.s3",
        "alluxio.proxy.s3.logging"};
    ResourceConfig config = new ResourceConfig().packages(packages)
        .register(JacksonProtobufObjectMapperProvider.class)
        .register(S3RestExceptionMapper.class);

    mFileSystem = FileSystem.Factory.create(Configuration.global());

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
          logAccess(httpReq, httpRes, stopWatch, null);
        }
      }
    };

    super.getServerConnector().addBean(new ProxyListener());
    if (Configuration.getBoolean(PropertyKey.PROXY_S3_V2_VERSION_ENABLED)) {
      ServletHolder s3ServletHolder = new ServletHolder("Alluxio Proxy V2 S3 Service",
          new S3RequestServlet() {
            @Override
            public void init() throws ServletException {
              super.init();
              getServletContext().setAttribute(ALLUXIO_PROXY_SERVLET_RESOURCE_KEY, proxyProcess);
              getServletContext()
                  .setAttribute(FILE_SYSTEM_SERVLET_RESOURCE_KEY, mFileSystem);
              getServletContext().setAttribute(STREAM_CACHE_SERVLET_RESOURCE_KEY,
                  new StreamCache(Configuration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS)));
              getServletContext().setAttribute(ALLUXIO_PROXY_AUDIT_LOG_WRITER_KEY,
                  mAsyncAuditLogWriter);

              getServletContext().setAttribute(PROXY_S3_V2_LIGHT_POOL,
                  new ThreadPoolExecutor(8, 64, 0,
                  TimeUnit.SECONDS, new ArrayBlockingQueue<>(64 * 1024),
                  ThreadFactoryUtils.build("S3-LIGHTPOOL-%d", false)));
              getServletContext().setAttribute(PROXY_S3_V2_HEAVY_POOL,
                  new ThreadPoolExecutor(8, 64, 0,
                  TimeUnit.SECONDS, new ArrayBlockingQueue<>(64 * 1024),
                  ThreadFactoryUtils.build("S3-HEAVYPOOL-%d", false)));
              getServletContext().setAttribute(PROXY_S3_HANDLER_MAP, mS3HandlerMap);
            }
          });
      mServletContextHandler
          .addServlet(s3ServletHolder, PathUtils.concatPath(Constants.REST_API_PREFIX, "*"));
    } else {
      addHandler(new CompleteMultipartUploadHandler(mFileSystem, Constants.REST_API_PREFIX));
      ServletHolder rsServletHolder = new ServletHolder("Alluxio Proxy Web Service", servlet);
      mServletContextHandler
          .addServlet(rsServletHolder, PathUtils.concatPath(Constants.REST_API_PREFIX, "*"));
    }
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
   * @param opType
   */
  public static void logAccess(HttpServletRequest request, HttpServletResponse response,
                               Stopwatch stopWatch, S3BaseTask.OpType opType) {
    String contentLenStr = "None";
    if (request.getHeader("x-amz-decoded-content-length") != null) {
      contentLenStr = request.getHeader("x-amz-decoded-content-length");
    } else if (request.getHeader("Content-Length") != null) {
      contentLenStr = request.getHeader("Content-Length");
    }
    String accessLog = String.format("[ACCESSLOG] %s Request:%s - Status:%d "
                    + "- ContentLength:%s - Elapsed(ms):%d",
            (opType == null ? "" : opType), request, response.getStatus(),
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
