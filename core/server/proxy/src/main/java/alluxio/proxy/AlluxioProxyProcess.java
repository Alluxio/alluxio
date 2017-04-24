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
import alluxio.PropertyKey;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.ProxyWebServer;
import alluxio.web.WebServer;

import com.google.common.base.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class encapsulates the different worker services that are configured to run.
 */
@NotThreadSafe
public final class AlluxioProxyProcess implements ProxyProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioProxyProcess.class);

  /** The web server. */
  private WebServer mWebServer = null;

  /** Worker start time in milliseconds. */
  private final long mStartTimeMs;

  private final CountDownLatch mLatch;

  /**
   * Creates an instance of {@link AlluxioProxy}.
   */
  AlluxioProxyProcess() {
    mStartTimeMs = System.currentTimeMillis();
    mLatch = new CountDownLatch(1);
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
    mWebServer.start();
    mLatch.await();
  }

  @Override
  public void stop() throws Exception {
    if (mWebServer != null) {
      mWebServer.stop();
      mWebServer = null;
    }
    mLatch.countDown();
  }

  @Override
  public void waitForReady() {
    CommonUtils.waitFor(this + " to start", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return mWebServer != null && mWebServer.getServer().isRunning();
      }
    });
  }

  @Override
  public String toString() {
    return "Alluxio Proxy";
  }
}
