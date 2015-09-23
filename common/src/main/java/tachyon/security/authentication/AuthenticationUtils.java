/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.security.authentication;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.security.sasl.SaslException;

import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.security.LoginUser;
import tachyon.util.network.NetworkAddressUtils;

/**
 * This class is the main entry for Tachyon authentication. Based on different authentication types
 * specified in Tachyon configuration, it provides corresponding Thrift class for authenticated
 * connection between Client and Server.
 */
public final class AuthenticationUtils {
  /**
   * For server side, this method returns a TTransportFactory based on the auth type. It is used as
   * one argument to build a Thrift TServer. If the auth type is not supported or recognized, an
   * UnsupportedOperationException is thrown.
   *
   * @param tachyonConf Tachyon Configuration
   * @return a corresponding TTransportFactory
   * @throws SaslException if building a TransportFactory fails
   */
  public static TTransportFactory getServerTransportFactory(TachyonConf tachyonConf)
      throws SaslException {
    AuthType authType =
        tachyonConf.getEnum(Constants.TACHYON_SECURITY_AUTHENTICATION, AuthType.class);
    switch (authType) {
      case NOSASL:
        return new TFramedTransport.Factory();
      case SIMPLE:
        // intended to fall through
      case CUSTOM:
        return PlainSaslHelper.getPlainServerTransportFactory(authType, tachyonConf);
      case KERBEROS:
        throw new UnsupportedOperationException("Kerberos is not supported currently.");
      default:
        throw new UnsupportedOperationException("Unsupported authentication type: "
            + authType.getAuthName());
    }
  }

  /**
   * Create a transport per the connection options. Supported transport options are: NOSASL, SIMPLE,
   * CUSTOM, KERBEROS. With NOSASL as input, an unmodified TTransport is returned; with
   * SIMPLE/CUSTOM as input, a PlainClientTransport is returned; KERBEROS is not supported
   * currently. If the auth type is not supported or recognized, an UnsupportedOperationException is
   * thrown.
   *
   * @param tachyonConf Tachyon Configuration
   * @param serverAddress the server address which clients will connect to
   * @return a TTransport for client
   * @throws IOException if building a TransportFactory fails or user login fails
   */
  public static TTransport getClientTransport(TachyonConf tachyonConf,
      InetSocketAddress serverAddress) throws IOException {
    AuthType authType =
        tachyonConf.getEnum(Constants.TACHYON_SECURITY_AUTHENTICATION, AuthType.class);
    TTransport tTransport = AuthenticationUtils.createTSocket(serverAddress);
    switch (authType) {
      case NOSASL:
        return new TFramedTransport(tTransport);
      case SIMPLE:
        // indent to fall through after case SIMPLE
      case CUSTOM:
        String username = LoginUser.get(tachyonConf).getName();
        return PlainSaslHelper.getPlainClientTransport(username, "noPassword", tTransport);
      case KERBEROS:
        throw new SaslException("Kerberos is not supported currently.");
      default:
        throw new SaslException("Unsupported authentication type: " + authType.getAuthName());
    }
  }

  /**
   * Create a new Thrift socket what will connect to the given address
   *
   * @param address The given address to connect
   * @return An unconnected socket
   */
  public static TSocket createTSocket(InetSocketAddress address) {
    return new TSocket(NetworkAddressUtils.getFqdnHost(address), address.getPort());
  }

  private AuthenticationUtils() {} // prevent instantiation
}
