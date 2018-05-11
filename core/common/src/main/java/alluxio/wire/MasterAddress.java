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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The address of a master.
 */
@NotThreadSafe
public final class MasterAddress implements Serializable {
  private static final long serialVersionUID = 1645922245406539186L;

  private String mHost = "";
  private int mRpcPort;

  /**
   * Creates a new instance of {@link MasterAddress}.
   */
  public MasterAddress() {}

  /**
   * Creates a new instance of {@link MasterAddress} from thrift representation.
   *
   * @param masterAddress the thrift master address
   */
  protected MasterAddress(alluxio.thrift.MasterAddress masterAddress) {
    mHost = masterAddress.getHost();
    mRpcPort = masterAddress.getRpcPort();
  }

  /**
   * @return the host of the master
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
   * @param host the host to use
   * @return the master address
   */
  public MasterAddress setHost(String host) {
    Preconditions.checkNotNull(host, "host");
    mHost = host;
    return this;
  }

  /**
   * @param rpcPort the rpc port to use
   * @return the master address
   */
  public MasterAddress setRpcPort(int rpcPort) {
    mRpcPort = rpcPort;
    return this;
  }

  /**
   * @return a address of thrift construct
   */
  protected alluxio.thrift.MasterAddress toThrift() {
    return new alluxio.thrift.MasterAddress()
        .setHost(mHost).setRpcPort(mRpcPort);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MasterAddress)) {
      return false;
    }
    MasterAddress that = (MasterAddress) o;
    return mHost.equals(that.mHost)
        && mRpcPort == that.mRpcPort;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mHost, mRpcPort);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("host", mHost)
        .add("rpcPort", mRpcPort)
        .toString();
  }
}
