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

import org.eclipse.jetty.servlet.ServletHolder;

import com.google.common.base.Preconditions;

import tachyon.conf.TachyonConf;
import tachyon.master.MasterInfo;

/**
 * A master's UI web server
 */
public class MasterUIWebServer extends UIWebServer {

  public MasterUIWebServer(String serverName, InetSocketAddress address, MasterInfo masterInfo,
      TachyonConf conf) {
    super(serverName, address, conf);
    Preconditions.checkNotNull(masterInfo, "Master information cannot be null");

    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceGeneralServlet(masterInfo)),
        "/home");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceWorkersServlet(masterInfo)),
        "/workers");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceConfigurationServlet(masterInfo)),
        "/configuration");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceBrowseServlet(masterInfo)),
        "/browse");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceMemoryServlet(masterInfo)),
        "/memory");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceDependencyServlet(masterInfo)),
        "/dependency");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceDownloadServlet(masterInfo)),
        "/download");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceDownloadLocalServlet()),
        "/downloadLocal");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceBrowseLogsServlet(true)),
        "/browseLogs");
  }
}
