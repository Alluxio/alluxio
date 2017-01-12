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

package alluxio.proxy;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.ServerUtils;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.sink.MetricsServlet;
import alluxio.security.authentication.TransportProvider;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.ProxyWebServer;
import alluxio.web.WebServer;
import alluxio.web.WorkerWebServer;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.AlluxioWorkerService;
import alluxio.worker.DataServer;
import alluxio.worker.Worker;
import alluxio.worker.WorkerFactory;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.DefaultBlockWorker;
import alluxio.worker.file.DefaultFileSystemWorker;
import alluxio.worker.file.FileSystemWorker;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class encapsulates the different worker services that are configured to run.
 */
@NotThreadSafe
public final class DefaultAlluxioProxy implements AlluxioProxyService {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The web server. */
  private WebServer mWebServer = null;

  /** Worker start time in milliseconds. */
  private long mStartTimeMs;

  /**
   * Creates an instance of {@link AlluxioProxy}.
   */
  public DefaultAlluxioProxy() {
    mStartTimeMs = System.currentTimeMillis();
  }

  @Override
  public int getWebLocalPort() {
    return mWebServer.getLocalPort();
  }

  @Override
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  @Override
  public long getUptimeMs() {
    return System.currentTimeMillis() - mStartTimeMs;
  }

  @Override
  public void start() throws Exception {
    mWebServer = new ProxyWebServer(ServiceType.PROXY_WEB.getServiceName(),
        NetworkAddressUtils.getBindAddress(ServiceType.PROXY_WEB), this);
    // reset proxy web port
    Configuration.set(PropertyKey.PROXY_WEB_PORT, Integer.toString(mWebServer.getLocalPort()));
    // start web server
    mWebServer.start();
  }

  @Override
  public void stop() throws Exception {
    LOG.info("Stopping Alluxio proxy.");
    if (mWebServer != null) {
      mWebServer.stop();
      mWebServer = null;
    }
  }

  @Override
  public void waitForReady() {
    CommonUtils.waitFor("Alluxio proxy to start", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return mWebServer != null && mWebServer.getServer().isRunning();
      }
    });
  }
}
