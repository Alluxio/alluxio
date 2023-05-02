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
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;

import io.netty.handler.ssl.SslContext;

/**
 * Interface for providing SslContext instances for TLS support.
 */
public interface SslContextProvider {

  /**
   * Factory for creating context provider implementations.
   */
  class Factory {
    private Factory() {} // prevent instantiation

    /**
     * Creates and initializes {@link SslContextProvider} implementation
     * based on Alluxio configuration.
     *
     * @return the generated {@link SslContextProvider} instance
     */
    public static SslContextProvider create(AlluxioConfiguration conf) {
      SslContextProvider provider = CommonUtils.createNewClassInstance(
          conf.getClass(PropertyKey.NETWORK_TLS_SSL_CONTEXT_PROVIDER_CLASSNAME), null, null);
      provider.init(conf);
      return provider;
    }
  }

  /**
   * Initializes provider.
   *
   * @param conf Alluxio configuration
   */
  void init(AlluxioConfiguration conf);

  /**
   * @return Singleton Ssl context for client
   */
  SslContext getClientSslContext();

  /**
   * @return SSLContext for the server side of an SSL connection
   */
  SslContext getServerSSLContext();

  /**
   * @return Singleton Ssl context for self-signed client
   */
  SslContext getSelfSignedClientSslContext();

  /**
   * @return Singleton Ssl context for self-signed server
   */
  SslContext getSelfSignedServerSslContext();
}
