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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The network address of a worker.
 */
@PublicApi
@NotThreadSafe
public final class WorkerNetAddress implements Serializable {
  private static final long serialVersionUID = 5822347646342091434L;

  private String mHost = "";
  private int mRpcPort;
  private int mDataPort;
  private int mWebPort;
  private String mDomainSocketPath = "";

  /**
   * Creates a new instance of {@link WorkerNetAddress}.
   */
  public WorkerNetAddress() {}

  /**
   * Creates a new instance of {@link WorkerNetAddress} from thrift representation.
   *
   * @param workerNetAddress the thrift net address
   */
  protected WorkerNetAddress(alluxio.thrift.WorkerNetAddress workerNetAddress) {
    mHost = workerNetAddress.getHost();
    mRpcPort = workerNetAddress.getRpcPort();
    mDataPort = workerNetAddress.getDataPort();
    mWebPort = workerNetAddress.getWebPort();
    mDomainSocketPath = workerNetAddress.getDomainSocketPath();
  }

  /**
   * @return the host of the worker
   */
  public String getHost() {
    return mHost;
  }

  /**
   * @return the RPC port
   */
  public int getRpcPort() {
    return mRpcPort;
  }

  /**
   * @return the data port
   */
  public int getDataPort() {
    return mDataPort;
  }

  /**
   * @return the web port
   */
  public int getWebPort() {
    return mWebPort;
  }

  /**
   * @return the domain socket path
   */
  public String getDomainSocketPath() {
    return mDomainSocketPath;
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
   * @return a net address of thrift construct
   */
  protected alluxio.thrift.WorkerNetAddress toThrift() {
    return new alluxio.thrift.WorkerNetAddress(mHost, mRpcPort, mDataPort, mWebPort,
        mDomainSocketPath);
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
    return mHost.equals(that.mHost) && mRpcPort == that.mRpcPort && mDataPort == that.mDataPort
        && mWebPort == that.mWebPort && mDomainSocketPath.equals(that.mDomainSocketPath);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mHost, mDataPort, mRpcPort, mWebPort, mDomainSocketPath);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("host", mHost).add("rpcPort", mRpcPort)
        .add("dataPort", mDataPort).add("webPort", mWebPort)
        .add("domainSocketPath", mDomainSocketPath).toString();
  }
}
