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

package alluxio.master.transport;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Used to provide external proxy configuration to messaging servers/clients.
 */
public class GrpcMessagingProxy {
  private Map<InetSocketAddress, InetSocketAddress> mProxyConf;

  /**
   * Creates new proxy configuration.
   */
  public GrpcMessagingProxy() {
    mProxyConf = new HashMap<>();
  }

  /**
   * Adds a new proxy mapping.
   *
   * @param address source address that has been proxied
   * @param proxy proxy address for source address
   * @return the updated proxy configuration
   */
  public GrpcMessagingProxy addProxy(InetSocketAddress address, InetSocketAddress proxy) {
    mProxyConf.put(address, proxy);
    return this;
  }

  /**
   * @param address address to check for proxy configuration
   * @return {@code true} if given address has proxy configuration
   */
  public boolean hasProxyFor(InetSocketAddress address) {
    return mProxyConf.containsKey(address);
  }

  /**
   * @param address source address
   * @return proxy address for given address
   */
  public InetSocketAddress getProxyFor(InetSocketAddress address) {
    return mProxyConf.get(address);
  }
}
