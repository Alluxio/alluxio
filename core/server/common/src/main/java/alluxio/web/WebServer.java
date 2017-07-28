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
import alluxio.PropertyKey;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class that bootstraps and starts a web server.
 */
@NotThreadSafe
public abstract class WebServer {
  private static final Logger LOG = LoggerFactory.getLogger(WebServer.class);

  private final Server mServer;
  private final String mServiceName;
  private InetSocketAddress mAddress;
  private final ServerConnector mServerConnector;
  protected final WebAppContext mWebAppContext;

  /**
   * Creates a new instance of {@link WebServer}. It pairs URLs with servlets and sets the webapp
   * folder.
   *
   * @param serviceName name of the web service
   * @param address address of the server
   */
  public WebServer(String serviceName, InetSocketAddress address) {
    Preconditions.checkNotNull(serviceName, "Service name cannot be null");
    Preconditions.checkNotNull(address, "Server address cannot be null");

    mAddress = address;
    mServiceName = serviceName;

    QueuedThreadPool threadPool = new QueuedThreadPool();
    int webThreadCount = Configuration.getInt(PropertyKey.WEB_THREADS);

    // Jetty needs at least (1 + selectors + acceptors) threads.
    threadPool.setMinThreads(webThreadCount * 2 + 1);
    threadPool.setMaxThreads(webThreadCount * 2 + 100);

    mServer = new Server(threadPool);

    mServerConnector = new ServerConnector(mServer);
    mServerConnector.setPort(mAddress.getPort());
    mServerConnector.setHost(mAddress.getAddress().getHostAddress());

    mServer.addConnector(mServerConnector);

    // Open the connector here so we can resolve the port if we are selecting a free port.
    try {
      mServerConnector.open();
    } catch (IOException e) {
      Throwables.propagate(e);
    }

    System.setProperty("org.apache.jasper.compiler.disablejsr199", "false");

    mWebAppContext = new WebAppContext();
    mWebAppContext.setContextPath(AlluxioURI.SEPARATOR);
    File warPath = new File(Configuration.get(PropertyKey.WEB_RESOURCES));
    mWebAppContext.setWar(warPath.getAbsolutePath());

    // Set the ContainerIncludeJarPattern so that jetty examines these
    // container-path jars for tlds, web-fragments etc.
    // If you omit the jar that contains the jstl .tlds, the jsp engine will
    // scan for them instead.
    mWebAppContext.setAttribute(
        "org.eclipse.jetty.server.webapp.ContainerIncludeJarPattern",
        ".*/[^/]*servlet-api-[^/]*\\.jar$|.*/javax.servlet.jsp.jstl-.*\\.jar$"
         + "|.*/[^/]*taglibs.*\\.jar$");

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
    String bindHost = mServerConnector.getHost();
    return bindHost == null ? "0.0.0.0" : bindHost;
  }

  /**
   * Gets the actual port that the web server is listening on.
   *
   * @return the local port
   */
  public int getLocalPort() {
    return mServerConnector.getLocalPort();
  }

  /**
   * Shuts down the web server.
   */
  public void stop() throws Exception {
    // close all connectors and release all binding ports
    for (Connector connector : mServer.getConnectors()) {
      connector.stop();
    }

    mServer.stop();
  }

  /**
   * Starts the web server.
   */
  public void start() {
    try {
      mServer.start();
      LOG.info("{} started @ {}", mServiceName, mAddress);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
