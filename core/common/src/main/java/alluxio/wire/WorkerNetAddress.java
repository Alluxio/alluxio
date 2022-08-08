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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.swagger.annotations.ApiModelProperty;

import java.util.Collections;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The network address of a worker.
 */
@PublicApi
@ThreadSafe
public final class WorkerNetAddress {
  private final String mHost;
  private final int mDataPort;
  private final TieredIdentity mTieredIdentity;

  private final Optional<String> mContainerHost;
  private final Optional<Integer> mRpcPort;
  private final Optional<Integer> mWebPort;
  private final Optional<String> mDomainSocketPath;

  /**
   * Creates a new instance of {@link WorkerNetAddress}.
   */
  private WorkerNetAddress(String host, String containerHost,
      int rpcPort, int dataPort, int webPort,
      String domainSocketPath, TieredIdentity tieredIdentity) {
    Preconditions.checkArgument(host != null && !host.isEmpty(),
        "host should not be null or empty");
    Preconditions.checkArgument(dataPort != 0,
        "data port should not be 0");
    mHost = host;
    mContainerHost = Optional.ofNullable(containerHost);
    mRpcPort = rpcPort == 0 ? Optional.empty() : Optional.of(rpcPort);
    mDataPort = dataPort;
    mWebPort = webPort == 0 ? Optional.empty() : Optional.of(webPort);
    mDomainSocketPath = Optional.ofNullable(domainSocketPath);
    mTieredIdentity = tieredIdentity == null ? new TieredIdentity(Collections.singletonList(
        new TieredIdentity.LocalityTier(Constants.LOCALITY_NODE, mHost))) : tieredIdentity;
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
  public Optional<String> getContainerHost() {
    return mContainerHost;
  }

  /**
   * @return the RPC port
   */
  @ApiModelProperty(value = "Port of the worker's Rpc server for metadata operations")
  public Optional<Integer> getRpcPort() {
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
  public Optional<Integer> getWebPort() {
    return mWebPort;
  }

  /**
   * @return the domain socket path
   */
  @ApiModelProperty(value = "The domain socket path used by the worker, disabled if empty")
  public Optional<String> getDomainSocketPath() {
    return mDomainSocketPath;
  }

  /**
   * @return the tiered identity
   */
  @ApiModelProperty(value = "The worker's tier identity")
  public TieredIdentity getTieredIdentity() {
    return mTieredIdentity;
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
        && mRpcPort.equals(that.mRpcPort)
        && mDataPort == that.mDataPort
        && mWebPort.equals(that.mWebPort)
        && mDomainSocketPath.equals(that.mDomainSocketPath)
        && mTieredIdentity.equals(that.mTieredIdentity);
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

  /**
   * Creates a new worker net address builder.
   *
   * @param host the hostname of the worker
   * @param dataPort data port of the worker
   * @return a new worker net address builder
   */
  public static Builder newBuilder(String host, int dataPort) {
    return new Builder(host, dataPort);
  }

  /**
   * Creates a new worker net address builder.
   *
   * @param address the original address to build from
   * @return the new builder
   */
  public static Builder newBuilder(WorkerNetAddress address) {
    return new Builder(address);
  }

  /**
   * Builder for worker net adress.
   */
  public static final class Builder {
    private String mHost;
    private String mContainerHost;
    private int mRpcPort;
    private int mDataPort;
    private int mWebPort;
    private String mDomainSocketPath;
    private TieredIdentity mTieredIdentity;

    Builder(String host, int dataPort) {
      mHost = host;
      mDataPort = dataPort;
    }

    Builder(WorkerNetAddress address) {
      mHost = address.getHost();
      mDataPort = address.getDataPort();
      mTieredIdentity = address.getTieredIdentity();
      if (address.getContainerHost().isPresent()) {
        mContainerHost = address.getContainerHost().get();
      }
      if (address.getRpcPort().isPresent()) {
        mRpcPort = address.getRpcPort().get();
      }
      if (address.getWebPort().isPresent()) {
        mWebPort = address.getWebPort().get();
      }
      if (address.getDomainSocketPath().isPresent()) {
        mDomainSocketPath = address.getDomainSocketPath().get();
      }
    }

    /**
     * @param containerHost the host of node, if running in a container
     * @return the worker net address
     */
    public Builder setContainerHost(String containerHost) {
      Preconditions.checkArgument(containerHost != null && !containerHost.isEmpty(),
          "container host should not be null or empty");
      mContainerHost = containerHost;
      return this;
    }

    /**
     * @param rpcPort the rpc port to use
     * @return the worker net address
     */
    public Builder setRpcPort(int rpcPort) {
      mRpcPort = rpcPort;
      return this;
    }

    /**
     * @param webPort the web port to use
     * @return the worker net address
     */
    public Builder setWebPort(int webPort) {
      mWebPort = webPort;
      return this;
    }

    /**
     * @param domainSocketPath the domain socket path
     * @return the worker net address
     */
    public Builder setDomainSocketPath(String domainSocketPath) {
      Preconditions.checkArgument(domainSocketPath != null && !domainSocketPath.isEmpty(),
          "domain socket path should not be null or empty");
      mDomainSocketPath = domainSocketPath;
      return this;
    }

    /**
     * @param tieredIdentity the tiered identity
     * @return the worker net address
     */
    public Builder setTieredIdentity(TieredIdentity tieredIdentity) {
      Preconditions.checkNotNull(tieredIdentity);
      mTieredIdentity = tieredIdentity;
      return this;
    }

    /**
     * @return the worker net address
     */
    public WorkerNetAddress build() {
      return new WorkerNetAddress(mHost, mContainerHost, mRpcPort,
          mDataPort, mWebPort, mDomainSocketPath, mTieredIdentity);
    }
  }
}
