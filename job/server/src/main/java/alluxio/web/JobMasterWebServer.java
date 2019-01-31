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
import alluxio.master.AlluxioJobMasterProcess;
import alluxio.util.io.PathUtils;

import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletException;

/**
 * Job master web server.
 */
@NotThreadSafe
public final class JobMasterWebServer extends WebServer {

  public static final String ALLUXIO_JOB_MASTER_SERVLET_RESOURCE_KEY = "Alluxio Job Master";

  /**
   * Creates a new instance of {@link JobMasterWebServer}. It pairs URLs with servlets.
   *
   * @param serviceName name of the web service
   * @param address address of the server
   * @param jobMaster the job master
   */
  public JobMasterWebServer(String serviceName, InetSocketAddress address,
      final AlluxioJobMasterProcess jobMaster) {
    super(serviceName, address);

    // REST configuration
    ResourceConfig config = new ResourceConfig().packages("alluxio.master", "alluxio.master.job");
    // Override the init method to inject a reference to AlluxioJobMaster into the servlet context.
    // ServletContext may not be modified until after super.init() is called.
    ServletContainer servlet = new ServletContainer(config) {
      private static final long serialVersionUID = 7756010860672831556L;

      @Override
      public void init() throws ServletException {
        super.init();
        getServletContext().setAttribute(ALLUXIO_JOB_MASTER_SERVLET_RESOURCE_KEY, jobMaster);
      }
    };

    ServletHolder servletHolder = new ServletHolder("Alluxio Job Master Web Service", servlet);
    mServletContextHandler
        .addServlet(servletHolder, PathUtils.concatPath(Constants.REST_API_PREFIX, "*"));
  }
}
