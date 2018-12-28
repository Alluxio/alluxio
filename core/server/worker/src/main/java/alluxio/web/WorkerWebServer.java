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
import alluxio.util.io.PathUtils;
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Preconditions;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ErrorPageErrorHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletException;

/**
 * The Alluxio worker web server.
 */
@NotThreadSafe
public final class WorkerWebServer extends WebServer {

  public static final String ALLUXIO_WORKER_SERVLET_RESOURCE_KEY = "Alluxio Worker";

  /**
   * Creates a new instance of {@link WorkerWebServer}.
   *
   * @param webAddress the service address
   * @param workerProcess the Alluxio worker process
   * @param blockWorker block worker to manage blocks
   * @param connectHost the connect host for the web server
   * @param startTimeMs start time milliseconds
   */
  public WorkerWebServer(InetSocketAddress webAddress, final WorkerProcess workerProcess,
      BlockWorker blockWorker, String connectHost, long startTimeMs) {
    super("Alluxio worker web service", webAddress);
    Preconditions.checkNotNull(blockWorker, "Block worker cannot be null");
    Logger LOG = LoggerFactory.getLogger(WorkerWebServer.class);
    // REST configuration
    ResourceConfig config = new ResourceConfig().packages("alluxio.worker", "alluxio.worker.block");
    // Override the init method to inject a reference to AlluxioWorker into the servlet context.
    // ServletContext may not be modified until after super.init() is called.
    ServletContainer servlet = new ServletContainer(config) {
      private static final long serialVersionUID = -7586014404855912954L;

      @Override
      public void init() throws ServletException {
        super.init();
        getServletContext().setAttribute(ALLUXIO_WORKER_SERVLET_RESOURCE_KEY, workerProcess);
      }
    };

    ServletHolder servletHolder = new ServletHolder("Alluxio Worker Web Service", servlet);
    mWebAppContext.addServlet(servletHolder, PathUtils.concatPath(Constants.REST_API_PREFIX, "*"));

    // STATIC assets
    try {
      String resourceDirPathString = "alluxio-ui/worker/build/";
      ClassLoader cl = WorkerWebServer.class.getClassLoader();
      URL resourceDir = cl.getResource(resourceDirPathString);
      URI webRootUri = resourceDir.toURI();
      mWebAppContext.setBaseResource(Resource.newResource(webRootUri));
      mWebAppContext.setWelcomeFiles(new String[] {"index.html"});
      mWebAppContext.setResourceBase(resourceDirPathString);
      mWebAppContext.addServlet(DefaultServlet.class, "/");
      ErrorPageErrorHandler errorHandler = new ErrorPageErrorHandler();
      errorHandler.addErrorPage(404, "/"); // TODO: william - consider a rewrite rule instead of an error handler
      mWebAppContext.setErrorHandler(errorHandler);
    } catch (URISyntaxException e) {
      LOG.error("ERROR: unable to set base resource path", e);
    } catch (MalformedURLException e) {
      LOG.error("ERROR: resource path is malformed", e);
    }
  }
}
