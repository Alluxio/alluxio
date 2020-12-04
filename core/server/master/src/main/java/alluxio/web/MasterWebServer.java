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
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.AlluxioMasterProcess;
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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletException;

/**
 * The Alluxio master web server.
 */
@NotThreadSafe
public final class MasterWebServer extends WebServer {
  private static final Logger LOG = LoggerFactory.getLogger(MasterWebServer.class);

  public static final String ALLUXIO_MASTER_SERVLET_RESOURCE_KEY = "Alluxio Master";
  public static final String ALLUXIO_FILESYSTEM_CLIENT_RESOURCE_KEY =
      "Alluxio Master FileSystem client";

  private final FileSystem mFileSystem;

  /**
   * Creates a new instance of {@link MasterWebServer}.
   *
   * @param serviceName the service name
   * @param address the service address
   * @param masterProcess the Alluxio master process
   */
  public MasterWebServer(String serviceName, InetSocketAddress address,
      final AlluxioMasterProcess masterProcess) {
    super(serviceName, address);
    Preconditions.checkNotNull(masterProcess, "Alluxio master cannot be null");
    // REST configuration
    ResourceConfig config = new ResourceConfig()
        .packages("alluxio.master", "alluxio.master.block", "alluxio.master.file")
        .register(JacksonProtobufObjectMapperProvider.class);
    mFileSystem = FileSystem.Factory.create(ServerConfiguration.global());
    // Override the init method to inject a reference to AlluxioMaster into the servlet context.
    // ServletContext may not be modified until after super.init() is called.
    ServletContainer servlet = new ServletContainer(config) {
      private static final long serialVersionUID = 7756010860672831556L;

      @Override
      public void init() throws ServletException {
        super.init();
        getServletContext().setAttribute(ALLUXIO_MASTER_SERVLET_RESOURCE_KEY, masterProcess);
        getServletContext().setAttribute(ALLUXIO_FILESYSTEM_CLIENT_RESOURCE_KEY, mFileSystem);
      }
    };

    ServletHolder servletHolder = new ServletHolder("Alluxio Master Web Service", servlet);
    mServletContextHandler
        .addServlet(servletHolder, PathUtils.concatPath(Constants.REST_API_PREFIX, "*"));

    // STATIC assets
    try {
      // If the Web UI is disabled, disable the resources and servlet together.
      if (ServerConfiguration.getBoolean(PropertyKey.WEB_UI_ENABLED)) {
        String resourceDirPathString =
                ServerConfiguration.get(PropertyKey.WEB_RESOURCES) + "/master/build/";
        File resourceDir = new File(resourceDirPathString);
        mServletContextHandler.setBaseResource(Resource.newResource(resourceDir.getAbsolutePath()));
        mServletContextHandler.setWelcomeFiles(new String[]{"index.html"});
        mServletContextHandler.setResourceBase(resourceDir.getAbsolutePath());
        mServletContextHandler.addServlet(DefaultServlet.class, "/");
        ErrorPageErrorHandler errorHandler = new ErrorPageErrorHandler();
        // TODO(william): consider a rewrite rule instead of an error handler
        errorHandler.addErrorPage(404, "/");
        mServletContextHandler.setErrorHandler(errorHandler);
      }
    } catch (IOException e) {
      LOG.error("ERROR: resource path is malformed", e);
    }
  }

  @Override
  public void stop() throws Exception {
    mFileSystem.close();
    super.stop();
  }
}
