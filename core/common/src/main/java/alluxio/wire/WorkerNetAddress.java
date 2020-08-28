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

import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.wire.TieredIdentity.LocalityTier;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;
import java.util.Arrays;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The network address of a worker.
 */
@PublicApi
@NotThreadSafe
public final class WorkerNetAddress implements Serializable {
  private static final long serialVersionUID = 0L;

  private String mHost = "";
  private String mContainerHost = "";
  private int mRpcPort;
  private int mDataPort;
  private int mWebPort;
  private String mDomainSocketPath = "";
  private TieredIdentity mTieredIdentity;

  /**
   * Creates a new instance of {@link WorkerNetAddress}.
   */
  public WorkerNetAddress() {}

  /**
   * @return the host of the worker
   */
  @ApiModelProperty(value = "Host name of the worker")
  public String getHost() {
    return mHost;
  }

  /**
   * @return the container host of the worker, default to empty string if the worker
   * is not in a container
   */
  @ApiModelProperty(value = "Host name of the physical node if running in a container")
  public String getContainerHost() {
    return mContainerHost;
  }

  /**
   * @return the RPC port
   */
  @ApiModelProperty(value = "Port of the worker's Rpc server for metadata operations")
  public int getRpcPort() {
    return mRpcPort;
  }

  /**
   * @return the data port
   */
  @ApiModelProperty(value = "Port of the worker's server for data operations")
  public int getDataPort() {
    return mDataPort;
  }

  /**
   * @return the web port
   */
  @ApiModelProperty(value = "Port which exposes the worker's web UI")
  public int getWebPort() {
    return mWebPort;
  }

  /**
   * @return the domain socket path
   */
  @ApiModelProperty(value = "The domain socket path used by the worker, disabled if empty")
  public String getDomainSocketPath() {
    return mDomainSocketPath;
  }

  /**
   * @return the tiered identity
   */
  @ApiModelProperty(value = "The worker's tier identity")
  public TieredIdentity getTieredIdentity() {
    if (mTieredIdentity != null) {
      return mTieredIdentity;
    }
    return new TieredIdentity(Arrays.asList(new LocalityTier(Constants.LOCALITY_NODE, mHost)));
  }

  /**
   * @param host the host to use
   * @return the worker net address
   */
  public WorkerNetAddress setHost(String host) {
    Preconditions.checkNotNull(host, "host");
    mHost = host;
    return this;
  }

  /**
   * @param containerHost the host of node, if running in a container
   * @return the worker net address
   */
  public WorkerNetAddress setContainerHost(String containerHost) {
    Preconditions.checkNotNull(containerHost, "containerHost");
    mContainerHost = containerHost;
    return this;
  }

  /**
   * @param rpcPort the rpc port to use
   * @return the worker net address
   */
  public WorkerNetAddress setRpcPort(int rpcPort) {
    mRpcPort = rpcPort;
    return this;
  }

  /**
   * @param dataPort the data port to use
   * @return the worker net address
   */
  public WorkerNetAddress setDataPort(int dataPort) {
    mDataPort = dataPort;
    return this;
  }

  /**
   * @param webPort the web port to use
   * @return the worker net address
   */
  public WorkerNetAddress setWebPort(int webPort) {
    mWebPort = webPort;
    return this;
  }

  /**
   * @param domainSocketPath the domain socket path
   * @return the worker net address
   */
  public WorkerNetAddress setDomainSocketPath(String domainSocketPath) {
    mDomainSocketPath = domainSocketPath;
    return this;
  }

  /**
   * @param tieredIdentity the tiered identity
   * @return the worker net address
   */
  public WorkerNetAddress setTieredIdentity(TieredIdentity tieredIdentity) {
    mTieredIdentity = tieredIdentity;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WorkerNetAddress)) {
      return false;
    }
    WorkerNetAddress that = (WorkerNetAddress) o;
    return mHost.equals(that.mHost)
        && mContainerHost.equals(that.mContainerHost)
        && mRpcPort == that.mRpcPort
        && mDataPort == that.mDataPort
        && mWebPort == that.mWebPort
        && mDomainSocketPath.equals(that.mDomainSocketPath)
        && Objects.equal(mTieredIdentity, that.mTieredIdentity);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mHost, mContainerHost, mDataPort, mRpcPort, mWebPort,
        mDomainSocketPath, mTieredIdentity);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("host", mHost)
        .add("containerHost", mContainerHost)
        .add("rpcPort", mRpcPort)
        .add("dataPort", mDataPort)
        .add("webPort", mWebPort)
        .add("domainSocketPath", mDomainSocketPath)
        .add("tieredIdentity", mTieredIdentity)
        .toString();
  }
}
