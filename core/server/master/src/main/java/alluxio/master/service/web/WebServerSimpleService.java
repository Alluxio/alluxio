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
import alluxio.master.MasterProcess;
import alluxio.master.service.SimpleService;
import alluxio.web.WebServer;

import com.google.common.base.Preconditions;
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

  private final MasterProcess mMasterProcess;

  @Nullable @GuardedBy("this")
  private WebServer mWebServer = null;

  protected WebServerSimpleService(MasterProcess masterProcess) {
    mMasterProcess = masterProcess;
  }

  /**
   * @return whether the web server is serving or not
   */
  public synchronized boolean isServing() {
    return mWebServer != null && mWebServer.getServer().isRunning();
  }

  protected synchronized void startWebServer() {
    LOG.info("Starting web server.");
    Preconditions.checkState(mWebServer == null, "web server must not already exist");
    mWebServer = mMasterProcess.createWebServer();
    mWebServer.start();
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
        MasterProcess masterProcess) {
      if (masterProcess instanceof AlluxioMasterProcess
          && Configuration.getBoolean(PropertyKey.STANDBY_MASTER_WEB_ENABLED)) {
        return new AlwaysOnWebServerSimpleService(masterProcess);
      }
      return new PrimaryOnlyWebServerSimpleService(bindAddress, masterProcess);
    }
  }
}
