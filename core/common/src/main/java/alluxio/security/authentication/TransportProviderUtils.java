/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.security.authentication;

import alluxio.util.network.NetworkAddressUtils;

import org.apache.thrift.transport.TSocket;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class provides factory methods for authentication in Alluxio. Based on different
 * authentication types specified in Alluxio configuration, it provides corresponding Thrift class
 * for authenticated connection between Client and Server.
 */
@ThreadSafe
public final class TransportProviderUtils {

  /**
   * Creates a new Thrift socket what will connect to the given address.
   *
   * @param address The given address to connect
   * @param timeoutMs the timeout in milliseconds
   * @return An unconnected socket
   */
  public static TSocket createThriftSocket(InetSocketAddress address, int timeoutMs) {
    return new TSocket(NetworkAddressUtils.getFqdnHost(address), address.getPort(), timeoutMs);
  }

  private TransportProviderUtils() {
  } // prevent instantiation
}
