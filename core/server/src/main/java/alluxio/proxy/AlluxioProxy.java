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
import alluxio.Server;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.ProxyWebServer;
import alluxio.web.WebServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Entry point for the Alluxio proxy program.
 */
@NotThreadSafe
public class AlluxioProxy implements Server {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Starts the Alluxio proxy.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioProxy.class.getCanonicalName());
      System.exit(-1);
    }

    // validate the configuration
    if (!ConfigurationUtils.validateConf()) {
      LOG.error("Invalid configuration found");
      System.exit(-1);
    }

    AlluxioProxy proxy = new AlluxioProxy();
    try {
      proxy.start();
    } catch (Exception e) {
      LOG.error("Uncaught exception while running Alluxio proxy, stopping it and exiting.", e);
      try {
        proxy.stop();
      } catch (Exception e2) {
        // continue to exit
        LOG.error("Uncaught exception while stopping Alluxio proxy, simply exiting.", e2);
      }
      System.exit(-1);
    }
  }

  /** The web server. */
  private WebServer mWebServer = null;

  private AlluxioProxy() {}

  /**
   * @return the actual bind hostname on web service (used by unit test only)
   */
  public String getWebBindHost() {
    if (mWebServer != null) {
      return mWebServer.getBindHost();
    }
    return "";
  }

  /**
   * @return the actual port that the web service is listening on (used by unit test only)
   */
  public int getWebLocalPort() {
    if (mWebServer != null) {
      return mWebServer.getLocalPort();
    }
    return -1;
  }

  @Override
  public void start() throws Exception {
    mWebServer = new ProxyWebServer(ServiceType.PROXY_WEB.getServiceName(),
        NetworkAddressUtils.getBindAddress(ServiceType.PROXY_WEB));
    // reset proxy web port
    Configuration.set(PropertyKey.PROXY_WEB_PORT, Integer.toString(mWebServer.getLocalPort()));
    // start web server
    mWebServer.start();
  }

  @Override
  public void stop() throws Exception {
    mWebServer.stop();
    mWebServer = null;
  }

  /**
   * Blocks until the proxy is ready to serve requests.
   */
  public void waitForReady() {
    while (true) {
      if (mWebServer != null && mWebServer.getServer().isRunning()) {
        return;
      }
      CommonUtils.sleepMs(10);
    }
  }
}
