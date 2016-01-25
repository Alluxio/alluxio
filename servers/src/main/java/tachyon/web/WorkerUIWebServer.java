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
import tachyon.util.network.NetworkAddressUtils.ServiceType;
import tachyon.worker.block.BlockDataManager;

/**
 * A worker's UI web server.
 */
@NotThreadSafe
public final class WorkerUIWebServer extends UIWebServer {

  /**
   * Creates a new instance of {@link WorkerUIWebServer}.
   *
   * @param serviceType the service type
   * @param webAddress the service address
   * @param blockdataManager block data manager
   * @param workerAddress the worker address
   * @param startTimeMs start time milliseconds
   * @param conf Tachyon configuration
   */
  public WorkerUIWebServer(ServiceType serviceType, InetSocketAddress webAddress,
      BlockDataManager blockdataManager, InetSocketAddress workerAddress, long startTimeMs,
      TachyonConf conf) {
    super(serviceType, webAddress, conf);
    Preconditions.checkNotNull(blockdataManager, "Block data manager cannot be null");
    Preconditions.checkNotNull(workerAddress, "Worker address cannot be null");

    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceWorkerGeneralServlet(
        blockdataManager, workerAddress, startTimeMs)), "/home");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceWorkerBlockInfoServlet(
        blockdataManager)), "/blockInfo");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceDownloadLocalServlet()),
        "/downloadLocal");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceBrowseLogsServlet(false)),
        "/browseLogs");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceHeaderServlet(conf)), "/header");
  }
}
