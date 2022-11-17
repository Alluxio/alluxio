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

package alluxio.master.service.web;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.AlluxioMasterProcess;
import alluxio.master.service.SimpleService;
import alluxio.network.RejectingServer;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.web.MasterWebServer;
import alluxio.web.WebServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * Defines common interface for the two difference web server behavior.
 */
public abstract class WebServerSimpleService implements SimpleService {
  private static final Logger LOG = LoggerFactory.getLogger(WebServerSimpleService.class);

  private final InetSocketAddress mBindAddress;
  private final AlluxioMasterProcess mMasterProcess;

  @Nullable @GuardedBy("this")
  private WebServer mWebServer = null;
  @Nullable @GuardedBy("this")
  private RejectingServer mRejectingServer = null;

  protected WebServerSimpleService(InetSocketAddress bindAddress,
      AlluxioMasterProcess masterProcess) {
    mBindAddress = bindAddress;
    mMasterProcess = masterProcess;
  }

  protected synchronized void startRejectingServer() {
    mRejectingServer = new RejectingServer(mBindAddress);
    mRejectingServer.start();
  }

  protected synchronized void stopRejectingServer() {
    if (mRejectingServer != null) {
      mRejectingServer.stopAndJoin();
      mRejectingServer = null;
    }
  }

  protected synchronized void startWebServer() {
    LOG.info("Starting web server.");
    mWebServer = new MasterWebServer(NetworkAddressUtils.ServiceType.MASTER_WEB.getServiceName(),
        mBindAddress, mMasterProcess);
  }

  protected synchronized void stopWebServer() {
    LOG.info("Stopping web server.");
    if (mWebServer != null) {
      try {
        mWebServer.stop();
      } catch (Exception e) {
        LOG.warn("Failed to stop web server", e);
      }
      mWebServer = null;
    }
  }

  /**
   * Factory for WebServer service.
   */
  public static class Factory {
    /**
     * @param bindAddress the address the web server will bind to
     * @param masterProcess the master process referenced inside the web server
     * @return a {@link WebServerSimpleService} that behaves according to
     * {@link alluxio.conf.PropertyKey#STANDBY_MASTER_WEB_ENABLED}
     */
    public static WebServerSimpleService create(InetSocketAddress bindAddress,
        AlluxioMasterProcess masterProcess) {
      if (Configuration.getBoolean(PropertyKey.STANDBY_MASTER_WEB_ENABLED)) {
        return new AlwaysOnWebServerSimpleService(bindAddress, masterProcess);
      }
      return new WhenLeadingWebServerSimpleService(bindAddress, masterProcess);
    }
  }
}
