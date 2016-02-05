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

package alluxio.web;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.jetty.servlet.ServletHolder;

import com.google.common.base.Preconditions;

import alluxio.Configuration;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.worker.block.BlockWorker;

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
   * @param blockWorker block worker to manage blocks
   * @param workerAddress the worker address
   * @param startTimeMs start time milliseconds
   * @param conf Alluxio configuration
   */
  public WorkerUIWebServer(ServiceType serviceType, InetSocketAddress webAddress,
      BlockWorker blockWorker, InetSocketAddress workerAddress, long startTimeMs,
      Configuration conf) {
    super(serviceType, webAddress, conf);
    Preconditions.checkNotNull(blockWorker, "Block Worker cannot be null");
    Preconditions.checkNotNull(workerAddress, "Worker address cannot be null");

    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceWorkerGeneralServlet(
        blockWorker, workerAddress, startTimeMs)), "/home");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceWorkerBlockInfoServlet(
        blockWorker)), "/blockInfo");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceDownloadLocalServlet()),
        "/downloadLocal");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceBrowseLogsServlet(false)),
        "/browseLogs");
    mWebAppContext.addServlet(new ServletHolder(new WebInterfaceHeaderServlet(conf)), "/header");
  }
}
