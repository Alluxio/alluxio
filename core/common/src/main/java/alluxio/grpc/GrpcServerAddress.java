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

package alluxio.grpc;

import com.google.common.base.MoreObjects;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;

/**
 * Defines an gRPC server endpoint.
 */
public class GrpcServerAddress {
  /** Physical address. */
  private SocketAddress mSocketAddress;
  /** Target host name. */
  private String mHostName;

  private GrpcServerAddress(String hostName, SocketAddress socketAddress) {
    mHostName = hostName;
    mSocketAddress = socketAddress;
  }

  /**
   * @param socketAddress physical address
   * @return created server address instance
   */
  public static GrpcServerAddress create(InetSocketAddress socketAddress) {
    return new GrpcServerAddress(socketAddress.getHostName(), socketAddress);
  }

  /**
   * @param hostName target host name
   * @param socketAddress physical address
   * @return created server address instance
   */
  public static GrpcServerAddress create(String hostName, SocketAddress socketAddress) {
    return new GrpcServerAddress(hostName, socketAddress);
  }

  /**
   * @return the host name
   */
  public String getHostName() {
    return mHostName;
  }

  /**
   * @return the socket address
   */
  public SocketAddress getSocketAddress() {
    return mSocketAddress;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(mHostName)
        .append(mSocketAddress)
        .toHashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof GrpcServerAddress)) {
      return false;
    }
    GrpcServerAddress otherAddress = (GrpcServerAddress) other;
    return Objects.equals(mHostName, otherAddress.getHostName())
        && Objects.equals(mSocketAddress, otherAddress.getSocketAddress());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("HostName", mHostName)
        .add("SocketAddress", mSocketAddress)
        .toString();
  }
}
