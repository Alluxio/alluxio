/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.grpc;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Defines an gRPC server endpoint.
 */
public class GrpcServerAddress {
  /** Physical address. */
  private SocketAddress mSocketAddress;
  /** Target host name. */
  private String mHostName;

  /**
   * @param socketAddress physical address
   */
  public GrpcServerAddress(InetSocketAddress socketAddress) {
    mHostName = socketAddress.getHostName();
    mSocketAddress = socketAddress;
  }

  /**
   * @param hostName target host name
   * @param socketAddress physical address
   */
  public GrpcServerAddress(String hostName, SocketAddress socketAddress) {
    mHostName = hostName;
    mSocketAddress = socketAddress;
  }

  /**
   * @return the host name
   */
  public String getHostName() {
    return mHostName;
  }

  /**
   * @return the host name
   */
  public SocketAddress getSocketAddress() {
    return mSocketAddress;
  }
}
