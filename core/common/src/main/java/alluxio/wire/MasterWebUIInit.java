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

package alluxio.wire;

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI overview information.
 */
@NotThreadSafe
public final class MasterWebUIInit implements Serializable {
  private static final long serialVersionUID = -6482980420852879864L;

  private boolean mDebug;
  private boolean mNewerVersionAvailable;
  private boolean mWebFileInfoEnabled;
  private boolean mSecurityAuthorizationPermissionEnabled;
  private int mWorkerPort;
  private int mRefreshInterval;
  private Map<String, String> mProxyDownloadFileApiUrl;

  /**
   * Creates a new instance of {@link MasterWebUIInit}.
   */
  public MasterWebUIInit() {
  }

  /**
   * Gets debug.
   *
   * @return the debug value
   */
  public boolean getDebug() {
    return mDebug;
  }

  /**
   * Gets newer version available.
   *
   * @return true if a newer version is available, false otherwise
   */
  public boolean getNewerVersionAvailable() {
    return mNewerVersionAvailable;
  }

  /**
   * Gets web file info enabled.
   *
   * @return the web file info enabled
   */
  public boolean getWebFileInfoEnabled() {
    return mWebFileInfoEnabled;
  }

  /**
   * Gets security authorization permission enabled.
   *
   * @return the security authorization permission enabled
   */
  public boolean getSecurityAuthorizationPermissionEnabled() {
    return mSecurityAuthorizationPermissionEnabled;
  }

  /**
   * Gets worker port.
   *
   * @return the worker port
   */
  public int getWorkerPort() {
    return mWorkerPort;
  }

  /**
   * Gets refresh interval.
   *
   * @return the refresh interval
   */
  public int getRefreshInterval() {
    return mRefreshInterval;
  }

  /**
   * Gets proxy download file api url.
   *
   * @return the proxy download file api url
   */
  public Map<String, String> getProxyDownloadFileApiUrl() {
    return mProxyDownloadFileApiUrl;
  }

  /**
   * Sets debug.
   *
   * @param debug the debug
   * @return the updated MasterWebUIInit instance
   */
  public MasterWebUIInit setDebug(boolean debug) {
    mDebug = debug;
    return this;
  }

  /**
   * Sets newer version available.
   *
   * @param newerVersionAvailable the newer version available
   * @return the updated MasterWebUIInit instance
   */
  public MasterWebUIInit setNewerVersionAvailable(boolean newerVersionAvailable) {
    mNewerVersionAvailable = newerVersionAvailable;
    return this;
  }

  /**
   * Sets web file info enabled.
   *
   * @param enabled the enabled
   * @return the updated MasterWebUIInit instance
   */
  public MasterWebUIInit setWebFileInfoEnabled(boolean enabled) {
    mWebFileInfoEnabled = enabled;
    return this;
  }

  /**
   * Sets security authorization permission enabled.
   *
   * @param enabled the enabled
   * @return the updated MasterWebUIInit instance
   */
  public MasterWebUIInit setSecurityAuthorizationPermissionEnabled(boolean enabled) {
    mSecurityAuthorizationPermissionEnabled = enabled;
    return this;
  }

  /**
   * Sets worker port.
   *
   * @param port the port
   * @return the updated MasterWebUIInit instance
   */
  public MasterWebUIInit setWorkerPort(int port) {
    mWorkerPort = port;
    return this;
  }

  /**
   * Sets refresh interval.
   *
   * @param interval the interval
   * @return the updated MasterWebUIInit instance
   */
  public MasterWebUIInit setRefreshInterval(int interval) {
    mRefreshInterval = interval;
    return this;
  }

  /**
   * Sets proxy download file api url.
   *
   * @param apiUrl the api url
   * @return the updated MasterWebUIInit instance
   */
  public MasterWebUIInit setProxyDownloadFileApiUrl(Map<String, String> apiUrl) {
    mProxyDownloadFileApiUrl = apiUrl;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("debug", mDebug)
        .add("newerVersionAvailable", mNewerVersionAvailable)
        .add("webFileInfoEnabled", mWebFileInfoEnabled)
        .add("securityAuthorizationPermissionEnabled", mSecurityAuthorizationPermissionEnabled)
        .add("workerPort", mWorkerPort).add("refreshInterval", mRefreshInterval).toString();
  }
}
