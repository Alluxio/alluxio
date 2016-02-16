/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.web;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.jetty.servlet.ServletHolder;

import com.google.common.base.Preconditions;

import tachyon.conf.TachyonConf;
import tachyon.master.TachyonMaster;
import tachyon.util.network.NetworkAddressUtils.ServiceType;

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
   * @param master the Tachyon master
   * @param conf the Tachyon configuration
   */
  public MasterUIWebServer(ServiceType service, InetSocketAddress address, TachyonMaster master,
      TachyonConf conf) {
    super(service, address, conf);
    Preconditions.checkNotNull(master, "TachyonMaster cannot be null");

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
    mWebAppContext.addServlet(
        new ServletHolder(new WebInterfaceMasterMetricsServlet(master.getMasterMetricsSystem())),
        "/metricsui");
  }
}
