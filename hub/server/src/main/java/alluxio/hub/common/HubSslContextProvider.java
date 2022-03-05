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

package alluxio.hub.common;

import alluxio.conf.AlluxioConfiguration;
import alluxio.util.network.tls.SslContextProvider;

import io.grpc.netty.GrpcSslContexts;
import io.netty.handler.ssl.SslContext;

import javax.net.ssl.SSLException;

/**
 * Hub SSL context provider.
 */
public class HubSslContextProvider implements SslContextProvider {

  @Override
  public void init(AlluxioConfiguration conf) {}

  @Override
  public SslContext getClientSslContext() {
    try {
      return GrpcSslContexts.forClient().build();
    } catch (SSLException e) {
      throw new RuntimeException("Unable to create Hub SSL context {}", e);
    }
  }

  @Override
  public SslContext getServerSSLContext() {
    throw new RuntimeException("Server SSL context not supported.");
  }

  @Override
  public SslContext getSelfSignedClientSslContext() {
    throw new RuntimeException("Self-signed Client SSL context not supported.");
  }

  @Override
  public SslContext getSelfSignedServerSslContext() {
    throw new RuntimeException("Self-signed Server SSL context not supported.");
  }
}
