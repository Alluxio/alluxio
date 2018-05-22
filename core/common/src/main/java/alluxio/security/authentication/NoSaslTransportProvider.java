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

package alluxio.security.authentication;

import alluxio.network.thrift.ThriftUtils;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * If authentication type is {@link AuthType#NOSASL}, we use this transport provider which simply
 * uses default Thrift {@link TTransport}.
 */
@ThreadSafe
public final class NoSaslTransportProvider implements TransportProvider {

  /**
   * Constructor for transport provider when authentication type is {@link AuthType#NOSASL}.
   */
  public NoSaslTransportProvider() {}

  @Override
  public TTransport getClientTransport(InetSocketAddress serverAddress) {
    return getClientTransport(null, serverAddress);
  }

  @Override
  public TTransport getClientTransport(Subject subject, InetSocketAddress serverAddress) {
    TTransport transport = ThriftUtils.createThriftSocket(serverAddress);
    return transport;
  }

  @Override
  public TTransportFactory getServerTransportFactory(String serverName) {
    return new TTransportFactory();
  }

  @Override
  public TTransportFactory getServerTransportFactory(Runnable runnable, String serverName) {
    return new TTransportFactory();
  }
}
