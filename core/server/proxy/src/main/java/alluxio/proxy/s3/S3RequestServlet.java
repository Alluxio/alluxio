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

package alluxio.proxy.s3;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.util.ThreadUtils;
import alluxio.web.ProxyWebServer;

import org.eclipse.jetty.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

/**
 * New architecture Request Servlet for handling s3 requests
 * in replacement of JAX-RS.
 */
public class S3RequestServlet extends HttpServlet {
  private static final long serialVersionUID = 2966302125671934038L;
  public static final String SERVICE_PREFIX = "s3";
  public static final String S3_V2_SERVICE_PATH_PREFIX = Constants.REST_API_PREFIX
      + AlluxioURI.SEPARATOR + SERVICE_PREFIX;
  private static final Logger LOG = LoggerFactory.getLogger(S3RequestServlet.class);
  /* (Experimental for new architecture enabled by PROXY_S3_V2_VERSION_ENABLED)
   * Processing threadpools for group of requests (for now, distinguish between
   * light-weighted metadata-centric requests and heavy io requests */
  public static final String PROXY_S3_V2_LIGHT_POOL = "Proxy S3 V2 Light Pool";
  public static final String PROXY_S3_V2_HEAVY_POOL = "Proxy S3 V2 Heavy Pool";

  /**
   * Implementation to serve the HttpServletRequest and returns HttpServletResponse.
   * @param request   the {@link HttpServletRequest} object that
   *                  contains the request the client made of
   *                  the servlet
   *
   * @param response  the {@link HttpServletResponse} object that
   *                  contains the response the servlet returns
   *                  to the client
   *
   * @throws ServletException
   * @throws IOException
   */
  @Override
  public void service(HttpServletRequest request,
                      HttpServletResponse response) throws ServletException, IOException {
    String target = request.getRequestURI();
    if (!target.startsWith(S3_V2_SERVICE_PATH_PREFIX)) {
      return;
    }
    S3Handler s3Handler = null;
    try {
      s3Handler = S3Handler.createHandler(target, request, response);
    } catch (Exception ex) {
      Response errorResponse = S3ErrorResponse.createErrorResponse(ex, "");
      S3Handler.processResponse(response, errorResponse);
      return;
    }
    ((ConcurrentHashMap<Request, S3Handler>) getServletContext()
        .getAttribute(ProxyWebServer.PROXY_S3_HANDLER_MAP))
        .put((Request) request, s3Handler);
    // Handle request async
    if (Configuration.getBoolean(PropertyKey.PROXY_S3_V2_ASYNC_PROCESSING_ENABLED)) {
      S3BaseTask.OpTag opTag = s3Handler.getS3Task().mOPType.getOpTag();
      ExecutorService es = (ExecutorService) (opTag == S3BaseTask.OpTag.LIGHT
          ? getServletContext().getAttribute(PROXY_S3_V2_LIGHT_POOL)
          : getServletContext().getAttribute(PROXY_S3_V2_HEAVY_POOL));

      final AsyncContext asyncCtx = request.startAsync();
      final S3Handler s3HandlerAsync = s3Handler;
      es.submit(() -> {
        try {
          serveRequest(s3HandlerAsync);
        } catch (Throwable th) {
          try {
            ((HttpServletResponse) asyncCtx.getResponse()).sendError(
                HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
          } catch (Throwable sendErrorEx) {
            LOG.error("Unexpected exception for {}/{}. {}", s3HandlerAsync.getBucket(),
                s3HandlerAsync.getObject(), ThreadUtils.formatStackTrace(sendErrorEx));
          }
        } finally {
          asyncCtx.complete();
        }
      });
    }
    // Handle request in current context
    else {
      serveRequest(s3Handler);
    }
  }

  /**
   * Core place to call S3 task's core API logic handling
   * function w/o exception handling.
   * @param s3Handler
   * @throws IOException
   */
  public void serveRequest(S3Handler s3Handler) throws IOException {
    if (s3Handler.getS3Task().getOPType() == S3BaseTask.OpType.CompleteMultipartUpload) {
      s3Handler.getS3Task().handleTaskAsync();
      return;
    }
    Response resp = s3Handler.getS3Task().continueTask();
    S3Handler.processResponse(s3Handler.getServletResponse(), resp);
  }
}
