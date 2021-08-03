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
import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.sink.MetricsServlet;
import alluxio.metrics.sink.PrometheusMetricsServlet;

import com.google.common.base.Preconditions;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class that bootstraps and starts a web server.
 */
@NotThreadSafe
public abstract class WebServer {
  private static final Logger LOG = LoggerFactory.getLogger(WebServer.class);
  private static final String DISABLED_METHODS = "TRACE,OPTIONS";

  private final Server mServer;
  private final String mServiceName;
  private final InetSocketAddress mAddress;
  private final ServerConnector mServerConnector;
  private final ConstraintSecurityHandler mSecurityHandler;
  protected final ServletContextHandler mServletContextHandler;
  private final MetricsServlet mMetricsServlet = new MetricsServlet(MetricsSystem.METRIC_REGISTRY);
  private final PrometheusMetricsServlet mPMetricsServlet = new PrometheusMetricsServlet(
      MetricsSystem.METRIC_REGISTRY);

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
    int webThreadCount = ServerConfiguration.getInt(PropertyKey.WEB_THREADS);

    // Jetty needs at least (1 + selectors + acceptors) threads.
    threadPool.setMinThreads(webThreadCount * 2 + 1);
    threadPool.setMaxThreads(webThreadCount * 2 + 100);

    mServer = new Server(threadPool);

    mServerConnector = new ServerConnector(mServer);
    mServerConnector.setPort(mAddress.getPort());
    mServerConnector.setHost(mAddress.getAddress().getHostAddress());
    mServerConnector.setReuseAddress(true);

    mServer.addConnector(mServerConnector);

    // Open the connector here so we can resolve the port if we are selecting a free port.
    try {
      mServerConnector.open();
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to listen on address %s for web server %s", mAddress, mServiceName),
          e);
    }

    System.setProperty("org.apache.jasper.compiler.disablejsr199", "false");
    mServletContextHandler = new ServletContextHandler(ServletContextHandler.SECURITY
        | ServletContextHandler.NO_SESSIONS);
    mServletContextHandler.setContextPath(AlluxioURI.SEPARATOR);

    // Disable specified methods on REST services
    mSecurityHandler = (ConstraintSecurityHandler) mServletContextHandler.getSecurityHandler();
    for (String s : DISABLED_METHODS.split(",")) {
      disableMethod(s);
    }

    mServletContextHandler.addServlet(StacksServlet.class, "/stacks");
    HandlerList handlers = new HandlerList();
    handlers.setHandlers(new Handler[] {mMetricsServlet.getHandler(), mPMetricsServlet.getHandler(),
        mServletContextHandler, new DefaultHandler()});
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
   * @param method to disable
   */
  private void disableMethod(String method) {
    Constraint constraint = new Constraint();
    constraint.setAuthenticate(true);
    constraint.setName("Disable " + method);
    ConstraintMapping disableMapping = new ConstraintMapping();
    disableMapping.setConstraint(constraint);
    disableMapping.setMethod(method.toUpperCase());
    disableMapping.setPathSpec("/");

    mSecurityHandler.addConstraintMapping(disableMapping);
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
      LOG.info("{} starting @ {}", mServiceName, mAddress);
      mServer.start();
      LOG.info("{} started @ {}", mServiceName, mAddress);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
