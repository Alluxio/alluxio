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

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI overview information.
 */
@NotThreadSafe
public final class WorkerWebUIInit implements Serializable {
  private static final long serialVersionUID = 5444572986825500764L;

  private boolean mDebug;
  private boolean mWebFileInfoEnabled;
  private boolean mSecurityAuthorizationPermissionEnabled;
  private String mMasterHostname;
  private int mMasterPort;
  private int mRefreshInterval;

  /**
   * Creates a new instance of {@link WorkerWebUIInit}.
   */
  public WorkerWebUIInit() {
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
   * Gets master ip.
   *
   * @return the master ip
   */
  public String getMasterHostname() {
    return mMasterHostname;
  }

  /**
   * Gets master port.
   *
   * @return the master port
   */
  public int getMasterPort() {
    return mMasterPort;
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
   * Sets debug.
   *
   * @param debug the debug
   * @return debug debug
   */
  public WorkerWebUIInit setDebug(boolean debug) {
    mDebug = debug;
    return this;
  }

  /**
   * Sets web file info enabled.
   *
   * @param enabled the enabled
   * @return the web file info enabled
   */
  public WorkerWebUIInit setWebFileInfoEnabled(boolean enabled) {
    mWebFileInfoEnabled = enabled;
    return this;
  }

  /**
   * Sets security authorization permission enabled.
   *
   * @param enabled the enabled
   * @return the security authorization permission enabled
   */
  public WorkerWebUIInit setSecurityAuthorizationPermissionEnabled(boolean enabled) {
    mSecurityAuthorizationPermissionEnabled = enabled;
    return this;
  }

  /**
   * Sets master ip.
   *
   * @param ip the ip
   * @return the master ip
   */
  public WorkerWebUIInit setMasterHostname(String ip) {
    mMasterHostname = ip;
    return this;
  }

  /**
   * Sets master port.
   *
   * @param port the port
   * @return the master port
   */
  public WorkerWebUIInit setMasterPort(int port) {
    mMasterPort = port;
    return this;
  }

  /**
   * Sets refresh interval.
   *
   * @param interval the interval
   * @return the refresh interval
   */
  public WorkerWebUIInit setRefreshInterval(int interval) {
    mRefreshInterval = interval;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("debug", mDebug)
        .add("webFileInfoEnabled", mWebFileInfoEnabled)
        .add("securityAuthorizationPermissionEnabled", mSecurityAuthorizationPermissionEnabled)
        .add("masterHostname", mMasterHostname).add("masterPort", mMasterPort)
        .add("refreshInterval", mRefreshInterval).toString();
  }
}
