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

import alluxio.Configuration;
import alluxio.Constants;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.security.sasl.SaslException;

/**
 * Interface to provide thrift transport service for thrift client and server, based on the type
 * of authentication.
 */
public interface TransportProvider {
  /**
   * Factory for {@code TransportProvider}.
   */
  class Factory {
    /**
     * Creates a new instance of {@code TransportProvider} based on authentication type. For
     * {@link AuthType#NOSASL}, return an instance of {@link NoSaslTransportProvider}; for
     * {@link AuthType#SIMPLE} or {@link AuthType#CUSTOM}, return an instance of
     * {@link PlainSaslTransportProvider}.
     *
     * @param conf Alluxio configuration
     * @return the generated {@link TransportProvider}
     */
    public static TransportProvider create(Configuration conf) {
      AuthType authType = conf.getEnum(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
      switch (authType) {
        case NOSASL:
          return new NoSaslTransportProvider(conf);
        case SIMPLE: // intended to fall through
        case CUSTOM:
          return new PlainSaslTransportProvider(conf);
        case KERBEROS:
          throw new UnsupportedOperationException(
              "getClientTransport: Kerberos is not supported currently.");
        default:
          throw new UnsupportedOperationException(
              "getClientTransport: Unsupported authentication type: " + authType.getAuthName());
      }
    }
  }

  /**
   * Creates a transport per the connection options. Supported transport options are:
   * {@link AuthType#NOSASL}, {@link AuthType#SIMPLE}, {link@ AuthType#CUSTOM},
   * {@link AuthType#KERBEROS}. With NOSASL as input, an unmodified {@link TTransport} is returned;
   * with SIMPLE/CUSTOM as input, a PlainClientTransport is returned; KERBEROS is not supported
   * currently. If the auth type is not supported or recognized, an
   * {@link UnsupportedOperationException} is thrown.
   *
   * @param serverAddress the server address which clients will connect to
   * @return a TTransport for client
   * @throws IOException if building a TransportFactory fails or user login fails
   */
  TTransport getClientTransport(InetSocketAddress serverAddress) throws IOException;

  /**
   * For server side, this method returns a {@link TTransportFactory} based on the auth type. It is
   * used as one argument to build a Thrift {@link org.apache.thrift.server.TServer}. If the auth
   * type is not supported or recognized, an {@link UnsupportedOperationException} is thrown.
   *
   * @return a corresponding TTransportFactory
   * @throws SaslException if building a TransportFactory fails
   */
  TTransportFactory getServerTransportFactory() throws SaslException;

}
