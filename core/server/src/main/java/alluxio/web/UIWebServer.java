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

import java.io.File;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import alluxio.Constants;
import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
/**
 * Class that bootstraps and starts the web server for the web interface.
 */
@NotThreadSafe
public abstract class UIWebServer {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  protected final WebAppContext mWebAppContext;
  private final Server mServer;
  private final ServiceType mService;
  private final Configuration mConfiguration;
  private InetSocketAddress mAddress;

  /**
   * Creates a new instance of {@link UIWebServer}. It pairs URLs with servlets and sets the webapp
   * folder.
   *
   * @param service name of the web service
   * @param address address of the server
   * @param conf Tachyon configuration
   */
  public UIWebServer(ServiceType service, InetSocketAddress address, Configuration conf) {
    Preconditions.checkNotNull(service, "Service type cannot be null");
    Preconditions.checkNotNull(address, "Server address cannot be null");
    Preconditions.checkNotNull(conf, "Configuration cannot be null");

    mAddress = address;
    mService = service;
    mConfiguration = conf;

    QueuedThreadPool threadPool = new QueuedThreadPool();
    int webThreadCount = mConfiguration.getInt(Constants.WEB_THREAD_COUNT);

    mServer = new Server();
    SelectChannelConnector connector = new SelectChannelConnector();
    connector.setHost(address.getHostName());
    connector.setPort(address.getPort());
    connector.setAcceptors(webThreadCount);
    mServer.setConnectors(new Connector[] {connector});

    // Jetty needs at least (1 + selectors + acceptors) threads.
    threadPool.setMinThreads(webThreadCount * 2 + 1);
    threadPool.setMaxThreads(webThreadCount * 2 + 100);
    mServer.setThreadPool(threadPool);

    mWebAppContext = new WebAppContext();
    mWebAppContext.setContextPath(AlluxioURI.SEPARATOR);
    File warPath = new File(mConfiguration.get(Constants.WEB_RESOURCES));
    mWebAppContext.setWar(warPath.getAbsolutePath());
    HandlerList handlers = new HandlerList();
    handlers.setHandlers(new Handler[] {mWebAppContext, new DefaultHandler()});
    mServer.setHandler(handlers);
  }

  /**
   * Adds a handler.
   *
   * @param handler the handler to add
   */
  public void addHandler(AbstractHandler handler) {
    HandlerList handlers = new HandlerList();
    handlers.addHandler(handler);
    for (Handler h : mServer.getHandlers()) {
      handlers.addHandler(h);
    }
    mServer.setHandler(handlers);
  }

  /**
   * @param handler to use
   */
  public void setHandler(AbstractHandler handler) {
    mServer.setHandler(handler);
  }

  /**
   * @return the underlying Jetty server
   */
  public Server getServer() {
    return mServer;
  }

  /**
   * Gets the actual bind hostname.
   *
   * @return the bind host
   */
  public String getBindHost() {
    String bindHost = mServer.getServer().getConnectors()[0].getHost();
    return bindHost == null ? "0.0.0.0" : bindHost;
  }

  /**
   * Gets the actual port that the web server is listening on.
   *
   * @return the local port
   */
  public int getLocalPort() {
    return mServer.getServer().getConnectors()[0].getLocalPort();
  }

  /**
   * Shuts down the web server.
   *
   * @throws Exception if the underlying jetty server throws an exception
   */
  public void shutdownWebServer() throws Exception {
    // close all connectors and release all binding ports
    for (Connector connector : mServer.getConnectors()) {
      connector.close();
    }

    mServer.stop();
  }

  /**
   * Starts the web server.
   */
  public void startWebServer() {
    try {
      mServer.getConnectors()[0].close();
      mServer.getConnectors()[0].open();
      mServer.start();
      if (mAddress.getPort() == 0) {
        int webPort = mServer.getConnectors()[0].getLocalPort();
        mAddress = new InetSocketAddress(mAddress.getHostName(), webPort);
        // reset web service port
        mConfiguration.set(mService.getPortKey(), Integer.toString(webPort));
      }
      LOG.info("{} started @ {}", mService.getServiceName(), mAddress);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
