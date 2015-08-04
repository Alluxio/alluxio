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

import java.util.Locale;

import javax.security.sasl.SaslException;

import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.security.PlainSaslHelper;

/**
 * This class is the main entry for Tachyon authentication.
 * It switches different modes based on configuration, and provides corresponding Thrift class
 * for authenticated connection between Client and Server.
 */
public class AuthenticationFactory {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Different authentication types for Tachyon.
   */
  public enum AuthType {
    /**
     * Authentication is disabled. No user info in Tachyon.
     */
    NOSASL("NOSASL"),

    /**
     * User is aware in Tachyon.
     * Login user is OS user. The verification of client user is disabled.
     */
    SIMPLE("SIMPLE"),

    /**
     * User is aware in Tachyon.
     * Login user is OS user. The user is verified by Custom authentication provider
     * (Use with property tachyon.authentication.provider.custom.class).
     */
    CUSTOM("CUSTOM"),

    /**
     * User is aware in Tachyon.
     * The user is verified by Kerberos authentication.
     */
    KERBEROS("KERBEROS");

    private final String mAuthType;

    AuthType(String authType) {
      mAuthType = authType;
    }

    public String getAuthName() {
      return mAuthType;
    }

    /**
     * Validate the authentication type string and return a corresponding AuthType.
     * @param authTypeStr authentication type string from configuration
     * @return the corresponding AuthType
     * @throws java.lang.IllegalArgumentException if the string does not match any type
     */
    public static AuthType getValidAuthType(String authTypeStr) {
      try {
        return AuthType.valueOf(authTypeStr.toUpperCase(Locale.ENGLISH));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(authTypeStr + " is not a valid authentication type. "
            + "Check the configuration parameter " + Constants.TACHYON_SECURITY_AUTHENTICATION, e);
      }
    }
  }

  private final AuthType mAuthType;
  private final TachyonConf mTachyonConf;

  public AuthenticationFactory(TachyonConf tachyonConf) {
    mTachyonConf = tachyonConf;
    mAuthType = getAuthTypeFromConf(tachyonConf);
  }

  AuthType getAuthType() {
    return mAuthType;
  }

  /**
   * Get an AuthType from the authentication type string in configuration
   * @param conf the TachyonConf
   * @return the corresponding AuthType
   * @throws java.lang.IllegalArgumentException if the string does not match any type.
   */
  public static AuthType getAuthTypeFromConf(TachyonConf conf) {
    // TODO: change the default value from NOSASL to SIMPLE, after feature is stable.
    return AuthType.getValidAuthType(conf.get(Constants.TACHYON_SECURITY_AUTHENTICATION,
        AuthType.NOSASL.getAuthName()));
  }

  /**
   * For server side, this method return a TTransportFactory based on the auth type. It is used as
   * one argument to build a Thrift TServer.
   * @return a corresponding TTransportFactory
   * @throws SaslException if building a TransportFactory fails
   */
  public TTransportFactory getServerTransportFactory() throws SaslException {
    switch (mAuthType) {
      case NOSASL:
        return new TFramedTransport.Factory();
      case SIMPLE:
      case CUSTOM:
        return PlainSaslHelper.getPlainServerTransportFactory(mAuthType, mTachyonConf);
      case KERBEROS:
        throw new UnsupportedOperationException("Kerberos is not supported currently.");
      default:
        throw new UnsupportedOperationException("Unsupported authentication type: " + mAuthType
            .getAuthName());
    }
  }

  // TODO: get client side TTransport based on auth type
  // public TTransport getClientTransport(InetSocketAddress serverAddress) throws SaslException {}
}
