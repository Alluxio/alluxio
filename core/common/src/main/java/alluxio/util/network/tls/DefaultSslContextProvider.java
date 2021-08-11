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

package alluxio.util.network.tls;

import alluxio.conf.AlluxioConfiguration;

import io.netty.handler.ssl.SslContext;

/**
 * Default SSL context provider.
 */
public class DefaultSslContextProvider implements SslContextProvider {

  @Override
  public void init(AlluxioConfiguration conf) {}

  @Override
  public SslContext getClientSslContext() {
    throw new RuntimeException("TLS not supported.");
  }

  @Override
  public SslContext getServerSSLContext() {
    throw new RuntimeException("TLS not supported.");
  }

  @Override
  public SslContext getSelfSignedClientSslContext() {
    throw new RuntimeException("TLS not supported.");
  }

  @Override
  public SslContext getSelfSignedServerSslContext() {
    throw new RuntimeException("TLS not supported.");
  }
}
