/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.web;

import alluxio.Configuration;
import alluxio.master.AlluxioMaster;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Preconditions;
import org.eclipse.jetty.servlet.ServletHolder;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A master's UI web server.
 */
@NotThreadSafe
public final class MasterUIWebServer extends UIWebServer {

  /**
   * Creates a new instance of {@link MasterUIWebServer}.
   *
   * @param service the service type
   * @param address the service address
   * @param master the Alluxio master
   * @param conf the Alluxio configuration
   */
  public MasterUIWebServer(ServiceType service, InetSocketAddress address, AlluxioMaster master,
      Configuration conf) {
    super(service, address, conf);
    Preconditions.checkNotNull(master, "AlluxioMaster cannot be null");

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
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceHeaderServlet(conf)),
        "/header");
  }
}
