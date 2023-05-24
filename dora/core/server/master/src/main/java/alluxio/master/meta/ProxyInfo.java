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

package alluxio.master.meta;

import alluxio.grpc.NetAddress;
import alluxio.util.CommonUtils;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Proxy information.
 */
@NotThreadSafe
public final class ProxyInfo {
  /** Proxy's address. */
  private final NetAddress mAddress;
  /** Proxy's last updated time in ms. */
  private long mLastHeartbeatTimeMs;
  /** Proxy's start time in ms. */
  private long mStartTimeMs = 0;
  /** Proxy's version. */
  private String mVersion = "";
  /** Proxy's revision. */
  private String mRevision = "";

  /**
   * Creates a new instance of {@link ProxyInfo}.
   *
   * @param address the proxy address to use
   */
  public ProxyInfo(NetAddress address) {
    mAddress = Preconditions.checkNotNull(address, "address");
    mLastHeartbeatTimeMs = CommonUtils.getCurrentMs();
  }

  /**
   * @return the proxy's address
   */
  public NetAddress getAddress() {
    return mAddress;
  }

  /**
   * @return the last updated time of the proxy in ms
   */
  public long getLastHeartbeatTimeMs() {
    return mLastHeartbeatTimeMs;
  }

  /**
   * @return the start time of the proxy in ms
   */
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  /**
   * @return the version of the proxy
   */
  public String getVersion() {
    return mVersion;
  }

  /**
   * @return the revision of the proxy
   */
  public String getRevision() {
    return mRevision;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("address", mAddress)
        .add("lastHeartbeatTimeMs", mLastHeartbeatTimeMs)
        .add("startTimeMs", mStartTimeMs)
        .add("version", mVersion)
        .add("revision", mRevision).toString();
  }

  /**
   * @param startTimeMs the start time of the proxy in ms
   */
  public void setStartTimeMs(long startTimeMs) {
    mStartTimeMs = startTimeMs;
  }

  /**
   * @param version the version of the proxy
   */
  public void setVersion(String version) {
    mVersion = version;
  }

  /**
   * @param revision the revision of the proxy
   */
  public void setRevision(String revision) {
    mRevision = revision;
  }

  /**
   * Updates the last updated time of the proxy in ms.
   */
  public void updateLastHeartbeatTimeMs() {
    mLastHeartbeatTimeMs = CommonUtils.getCurrentMs();
  }
}
