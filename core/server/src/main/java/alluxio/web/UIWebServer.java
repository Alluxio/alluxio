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

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;
/**
 * Class that bootstraps and starts the web server for the web interface.
 */
@NotThreadSafe
public abstract class UIWebServer {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  protected final WebAppContext mWebAppContext;
  private final Server mServer;
  private final ServiceType mService;
  private InetSocketAddress mAddress;

  /**
   * Creates a new instance of {@link UIWebServer}. It pairs URLs with servlets and sets the webapp
   * folder.
   *
   * @param service name of the web service
   * @param address address of the server
   */
  public UIWebServer(ServiceType service, InetSocketAddress address) {
    Preconditions.checkNotNull(service, "Service type cannot be null");
    Preconditions.checkNotNull(address, "Server address cannot be null");

    mAddress = address;
    mService = service;

    QueuedThreadPool threadPool = new QueuedThreadPool();
    int webThreadCount = Configuration.getInt(PropertyKey.WEB_THREADS);

    mServer = new Server();
    SelectChannelConnector connector = new SelectChannelConnector();
    connector.setHost(address.getHostName());
    connector.setPort(address.getPort());
    connector.setAcceptors(webThreadCount);
    mServer.setConnectors(new Connector[] {connector});

    try {
      mServer.getConnectors()[0].close();
      mServer.getConnectors()[0].open();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    // Jetty needs at least (1 + selectors + acceptors) threads.
    threadPool.setMinThreads(webThreadCount * 2 + 1);
    threadPool.setMaxThreads(webThreadCount * 2 + 100);
    mServer.setThreadPool(threadPool);

    mWebAppContext = new WebAppContext();
    mWebAppContext.setContextPath(AlluxioURI.SEPARATOR);
    File warPath = new File(Configuration.get(PropertyKey.WEB_RESOURCES));
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
      mServer.start();
      LOG.info("{} started @ {}", mService.getServiceName(), mAddress);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
