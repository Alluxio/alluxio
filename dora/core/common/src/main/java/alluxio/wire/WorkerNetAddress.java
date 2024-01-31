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

import alluxio.annotation.PublicApi;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The network address of a worker.
 */
@PublicApi
@NotThreadSafe
public final class WorkerNetAddress implements Serializable {
  private static final long serialVersionUID = 0L;

  public static final WorkerNetAddress DUMMY = new WorkerNetAddress();

  @Expose
  @com.google.gson.annotations.SerializedName("Host")
  private String mHost = "";
  @Expose
  @com.google.gson.annotations.SerializedName("ContainerHost")
  private String mContainerHost = "";
  @Expose
  @com.google.gson.annotations.SerializedName("RpcPort")
  private int mRpcPort;
  @Expose
  @com.google.gson.annotations.SerializedName("DataPort")
  private int mDataPort;
  @Expose
  @com.google.gson.annotations.SerializedName("SecureRpcPort")
  private int mSecureRpcPort;
  @Expose
  @com.google.gson.annotations.SerializedName("NettyDataPort")
  private int mNettyDataPort;
  @Expose
  @com.google.gson.annotations.SerializedName("WebPort")
  private int mWebPort;
  @Expose
  @com.google.gson.annotations.SerializedName("DomainSocketPath")
  private String mDomainSocketPath = "";
  @Expose
  @com.google.gson.annotations.SerializedName("HttpServerPort")
  // Optional field - skipped in the customized equality comparison
  private int mHttpServerPort;

  /**
   * Creates a new instance of {@link WorkerNetAddress}.
   */
  public WorkerNetAddress() {
  }

  /**
   * Copy constructor.
   *
   * @param copyFrom instance to copy from
   */
  public WorkerNetAddress(WorkerNetAddress copyFrom) {
    mHost = copyFrom.mHost;
    mContainerHost = copyFrom.mContainerHost;
    mRpcPort = copyFrom.mRpcPort;
    mDataPort = copyFrom.mDataPort;
    mSecureRpcPort = copyFrom.mSecureRpcPort;
    mNettyDataPort = copyFrom.mNettyDataPort;
    mWebPort = copyFrom.mWebPort;
    mDomainSocketPath = copyFrom.mDomainSocketPath;
    mHttpServerPort = copyFrom.mHttpServerPort;
  }

  /**
   * @return the secure rpc port
   */
  public int getSecureRpcPort() {
    return mSecureRpcPort;
  }

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
   * @return the netty data port
   */
  @ApiModelProperty(value = "Port of the worker's server for netty data operations")
  public int getNettyDataPort() {
    return mNettyDataPort;
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
   * @return the http server port
   */
  @ApiModelProperty(value = "Port of the worker's http server for rest apis")
  public int getHttpServerPort() {
    return mHttpServerPort;
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
   * @param nettyDataPort the netty data port to use
   * @return the worker net address
   */
  public WorkerNetAddress setNettyDataPort(int nettyDataPort) {
    mNettyDataPort = nettyDataPort;
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
   * @param secureRpcPort the secure rpc port port to use
   * @return the worker net address
   */
  public WorkerNetAddress setSecureRpcPort(int secureRpcPort) {
    mSecureRpcPort = secureRpcPort;
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
   * @param httpServerPort the http server port to use
   * @return the worker net address
   */
  public WorkerNetAddress setHttpServerPort(int httpServerPort) {
    mHttpServerPort = httpServerPort;
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
        && mSecureRpcPort == that.mSecureRpcPort
        && mRpcPort == that.mRpcPort
        && mDataPort == that.mDataPort
        && mWebPort == that.mWebPort
        && mDomainSocketPath.equals(that.mDomainSocketPath)
        && mHttpServerPort == that.mHttpServerPort;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSecureRpcPort, mHost, mContainerHost, mDataPort, mRpcPort, mWebPort,
        mDomainSocketPath, mHttpServerPort);
  }

  /**
   * dump the main info of the WorkerNetAddress object.
   *
   * @return the main info string of the WorkerNetAddress object
   */
  public String dumpMainInfo() {
    return MoreObjects.toStringHelper(this)
        .add("host", mHost)
        .add("containerHost", mContainerHost)
        .add("rpcPort", mRpcPort)
        .add("dataPort", mDataPort)
        .add("webPort", mWebPort)
        .add("domainSocketPath", mDomainSocketPath)
        .add("httpServerPort", mHttpServerPort)
        .toString();
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
        .add("secureRpcPort", mSecureRpcPort)
        .add("httpServerPort", mHttpServerPort)
        .toString();
  }
}
