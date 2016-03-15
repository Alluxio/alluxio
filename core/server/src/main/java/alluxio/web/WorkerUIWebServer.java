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
import alluxio.Constants;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.worker.AlluxioWorker;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Preconditions;
import org.eclipse.jetty.servlet.ServletHolder;

import java.net.InetSocketAddress;
import java.util.Arrays;

import javax.annotation.concurrent.NotThreadSafe;

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
   * @param alluxioWorker Alluxio worker
   * @param blockWorker block worker to manage blocks
   * @param workerAddress the worker address
   * @param startTimeMs start time milliseconds
   * @param conf Alluxio configuration
   */
  public WorkerUIWebServer(ServiceType serviceType, InetSocketAddress webAddress,
      AlluxioWorker alluxioWorker, BlockWorker blockWorker, InetSocketAddress workerAddress,
      long startTimeMs, Configuration conf) {
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
    mWebAppContext.addServlet(new ServletHolder(new
        WebInterfaceWorkerMetricsServlet(alluxioWorker.getWorkerMetricsSystem())), "/metrics");

    // REST configuration
    mWebAppContext.setOverrideDescriptors(Arrays.asList(conf.get(Constants.WEB_RESOURCES)
        + "/WEB-INF/worker.xml"));
  }
}
