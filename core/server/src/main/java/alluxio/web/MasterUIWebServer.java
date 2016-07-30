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

import alluxio.master.AlluxioMaster;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Preconditions;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletException;

/**
 * A master's UI web server.
 */
@NotThreadSafe
public final class MasterUIWebServer extends UIWebServer {

  public static final String ALLUXIO_MASTER_SERVLET_RESOURCE_KEY = "Alluxio Master";

  /**
   * Creates a new instance of {@link MasterUIWebServer}.
   *
   * @param service the service type
   * @param address the service address
   * @param master the Alluxio master
   */
  public MasterUIWebServer(ServiceType service, InetSocketAddress address, final AlluxioMaster master) {
    super(service, address);
    Preconditions.checkNotNull(master, "Alluxio master cannot be null");

    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceGeneralServlet(master)), "/home");
    mWebAppContext.addServlet(new ServletHolder(
        new WebInterfaceWorkersServlet(master.getBlockMaster())), "/workers");
    mWebAppContext.addServlet(new ServletHolder(
        new WebInterfaceConfigurationServlet(master.getFileSystemMaster())), "/configuration");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceBrowseServlet(master)), "/browse");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceMemoryServlet(master)), "/memory");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceDependencyServlet(master)),
        "/dependency");
    mWebAppContext.addServlet(new ServletHolder(
        new WebInterfaceDownloadServlet(master.getFileSystemMaster())), "/download");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceDownloadLocalServlet()),
        "/downloadLocal");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceBrowseLogsServlet(true)),
        "/browseLogs");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceHeaderServlet()),
        "/header");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceMasterMetricsServlet(
        master.getMasterMetricsSystem())), "/metricsui");
    // REST configuration
    ResourceConfig config = new ResourceConfig().packages("alluxio.master", "alluxio.master.block",
        "alluxio.master.file", "alluxio.master.lineage");
    ServletContainer servlet = new ServletContainer(config) {
      private static final long serialVersionUID = 7756010860672831556L;

      @Override
      public void init() throws ServletException {
        super.init();
        getServletContext().setAttribute(ALLUXIO_MASTER_SERVLET_RESOURCE_KEY, master);
      }
    };

    /*
    ServletContextListener listener = new ServletContextListener() {
      public void contextInitialized(ServletContextEvent event) {
        event.getServletContext()
      }

      @Override
      public void contextDestroyed(ServletContextEvent event) {}
    };*/

    ServletHolder servletHolder =
        new ServletHolder("Alluxio Master Web Service", servlet);
    mWebAppContext.addServlet(servletHolder, "/v1/api/*");
  }
}
