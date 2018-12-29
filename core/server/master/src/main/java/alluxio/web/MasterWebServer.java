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
import alluxio.master.MasterProcess;
import alluxio.master.file.FileSystemMaster;
import alluxio.util.io.PathUtils;

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
 * The Alluxio master web server.
 */
@NotThreadSafe
public final class MasterWebServer extends WebServer {
  private static final Logger LOG = LoggerFactory.getLogger(MasterWebServer.class);

  public static final String ALLUXIO_MASTER_SERVLET_RESOURCE_KEY = "Alluxio Master";

  /**
   * Creates a new instance of {@link MasterWebServer}.
   *
   * @param serviceName the service name
   * @param address the service address
   * @param masterProcess the Alluxio master process
   */
  public MasterWebServer(String serviceName, InetSocketAddress address,
      final MasterProcess masterProcess) {
    super(serviceName, address);
    Preconditions.checkNotNull(masterProcess, "Alluxio master cannot be null");
    mWebAppContext
        .addServlet(new ServletHolder(// TODO(william): migrate this into a REST api endpoint
                new WebInterfaceDownloadServlet(masterProcess.getMaster(FileSystemMaster.class))),
            "/download");
    // REST configuration
    ResourceConfig config = new ResourceConfig()
        .packages("alluxio.master", "alluxio.master.block", "alluxio.master.file");
    // Override the init method to inject a reference to AlluxioMaster into the servlet context.
    // ServletContext may not be modified until after super.init() is called.
    ServletContainer servlet = new ServletContainer(config) {
      private static final long serialVersionUID = 7756010860672831556L;

      @Override
      public void init() throws ServletException {
        super.init();
        getServletContext().setAttribute(ALLUXIO_MASTER_SERVLET_RESOURCE_KEY, masterProcess);
      }
    };

    ServletHolder servletHolder = new ServletHolder("Alluxio Master Web Service", servlet);
    mWebAppContext.addServlet(servletHolder, PathUtils.concatPath(Constants.REST_API_PREFIX, "*"));

    // STATIC assets
    try {
      String resourceDirPathString = "alluxio-ui/master/build/";
      ClassLoader cl = MasterWebServer.class.getClassLoader();
      URL resourceDir = cl.getResource(resourceDirPathString);
      URI webRootUri = resourceDir.toURI();
      mWebAppContext.setBaseResource(Resource.newResource(webRootUri));
      mWebAppContext.setWelcomeFiles(new String[] {"index.html"});
      mWebAppContext.setResourceBase(resourceDirPathString);
      mWebAppContext.addServlet(DefaultServlet.class, "/");
      ErrorPageErrorHandler errorHandler = new ErrorPageErrorHandler();
      errorHandler.addErrorPage(404,
          "/"); // TODO(william): consider a rewrite rule instead of an error handler
      mWebAppContext.setErrorHandler(errorHandler);
    } catch (URISyntaxException e) {
      LOG.error("ERROR: unable to set base resource path", e);
    } catch (MalformedURLException e) {
      LOG.error("ERROR: resource path is malformed", e);
    }
  }
}
