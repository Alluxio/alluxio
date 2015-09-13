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
 * This class is the main entry for Tachyon authentication. It switches different modes based on
 * configuration, and provides corresponding Thrift class for authenticated connection between
 * Client and Server.
 */
public final class AuthenticationFactory {
  /**
   * Different authentication types for Tachyon.
   */
  public enum AuthType {
    /**
     * Authentication is disabled. No user info in Tachyon.
     */
    NOSASL("NOSASL"),

    /**
     * User is aware in Tachyon. Login user is OS user. The verification of client user is disabled.
     */
    SIMPLE("SIMPLE"),

    /**
     * User is aware in Tachyon. Login user is OS user. The user is verified by Custom
     * authentication provider (Use with property tachyon.authentication.provider.custom.class).
     */
    CUSTOM("CUSTOM"),

    /**
     * User is aware in Tachyon. The user is verified by Kerberos authentication. NOTE: this
     * authentication is not supported.
     */
    KERBEROS("KERBEROS");

    private final String mAuthType;

    AuthType(String authType) {
      mAuthType = authType;
    }

    public String getAuthName() {
      return mAuthType;
    }
  }

  private final AuthType mAuthType;
  private final TachyonConf mTachyonConf;

  public AuthenticationFactory(TachyonConf tachyonConf) {
    mTachyonConf = tachyonConf;
    mAuthType =
        tachyonConf.getEnum(Constants.TACHYON_SECURITY_AUTHENTICATION,
            AuthenticationFactory.AuthType.class);
  }

  public AuthType getAuthType() {
    return mAuthType;
  }

  /**
   * For server side, this method return a TTransportFactory based on the auth type. It is used as
   * one argument to build a Thrift TServer. If the auth type is not supported or recognized, an
   * UnsupportedOperationException is thrown.
   *
   * @return a corresponding TTransportFactory
   * @throws SaslException if building a TransportFactory fails
   */
  public TTransportFactory getServerTransportFactory() throws SaslException {
    switch (mAuthType) {
      case NOSASL:
        return new TFramedTransport.Factory();
      case SIMPLE:
        // intent to fall through
      case CUSTOM:
        return PlainSaslHelper.getPlainServerTransportFactory(mAuthType, mTachyonConf);
      case KERBEROS:
        throw new UnsupportedOperationException("Kerberos is not supported currently.");
      default:
        throw new UnsupportedOperationException("Unsupported authentication type: "
            + mAuthType.getAuthName());
    }
  }

  /**
   * Create a transport per the connection options. Supported transport options are: NOSASL, SIMPLE,
   * CUSTOM, KERBEROS. With NOSASL as input, an unmodified TTransport is returned; with
   * SIMPLE/CUSTOM as input, a PlainClientTransport is returned; KERBEROS is not supported
   * currently. If the auth type is not supported or recognized, an UnsupportedOperationException is
   * thrown.
   *
   * @param serverAddress the server address which clients will connect to
   * @return a TTransport for client
   * @throws IOException if building a TransportFactory fails or user login fails
   */
  public TTransport getClientTransport(InetSocketAddress serverAddress) throws IOException {
    TTransport tTransport = AuthenticationFactory.createTSocket(serverAddress);
    switch (mAuthType) {
      case NOSASL:
        return new TFramedTransport(tTransport);
      case SIMPLE:
        // indent to fall through after case SIMPLE
      case CUSTOM:
        String username = LoginUser.get(mTachyonConf).getName();
        return PlainSaslHelper.getPlainClientTransport(username, "noPassword", tTransport);
      case KERBEROS:
        throw new SaslException("Kerberos is not supported currently.");
      default:
        throw new SaslException("Unsupported authentication type: " + mAuthType.getAuthName());
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

}
