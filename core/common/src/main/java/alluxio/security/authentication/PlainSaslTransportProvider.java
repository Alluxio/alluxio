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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.network.thrift.ThriftUtils;
import alluxio.security.LoginUser;
import alluxio.security.User;

import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.net.InetSocketAddress;
import java.security.Security;
import java.util.HashMap;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;
import javax.security.sasl.SaslException;

/**
 * If authentication type is {@link AuthType#SIMPLE} or {@link AuthType#CUSTOM}, this is the
 * default transport provider which uses SASL transport.
 */
@ThreadSafe
public final class PlainSaslTransportProvider implements TransportProvider {
  static {
    Security.addProvider(new PlainSaslServerProvider());
  }

  /**
   * Constructor for transport provider with {@link AuthType#SIMPLE} or {@link AuthType#CUSTOM}.
   */
  public PlainSaslTransportProvider() {}

  @Override
  public TTransport getClientTransport(InetSocketAddress serverAddress)
      throws UnauthenticatedException {
    return getClientTransport(null, ThriftUtils.createThriftSocket(serverAddress));
  }

  @Override
  public TTransport getClientTransport(Subject subject, TTransport baseTransport)
      throws UnauthenticatedException {
    String username = null;
    String password = "noPassword";

    if (subject != null) {
      Set<User> user = subject.getPrincipals(User.class);
      if (user != null && !user.isEmpty()) {
        username = user.iterator().next().getName();
      }
    }
    if (username == null || username.isEmpty()) {
      username = LoginUser.get().getName();
    }

    // Determine the impersonation user
    String impersonationUser = TransportProviderUtils.getImpersonationUser(subject);

    if (impersonationUser != null && Configuration
        .isSet(PropertyKey.SECURITY_LOGIN_IMPERSONATION_USERNAME)
        && Constants.IMPERSONATION_HDFS_USER
        .equals(Configuration.get(PropertyKey.SECURITY_LOGIN_IMPERSONATION_USERNAME))) {
      // If impersonation is configured to use the HDFS user, the connection user should
      // be not be the HDFS user, but the LoginUser.
      // If the HDFS user is really supposed to be the connection user, that can be achieved by
      // not enabling impersonation for the client.
      username = LoginUser.get().getName();
    }
    return getClientTransport(username, password, impersonationUser, baseTransport);
  }

  // TODO(binfan): make this private and use whitebox to access this method in test
  /**
   * Gets a PLAIN mechanism transport for client side.
   *
   * @param username User Name of PlainClient
   * @param password Password of PlainClient
   * @param impersonationUser impersonation user (not used if null)
   * @param baseTransport base transport
   * @return Wrapped transport with PLAIN mechanism
   */
  public TTransport getClientTransport(String username, String password, String impersonationUser,
      TTransport baseTransport) throws UnauthenticatedException {
    try {
      return new TSaslClientTransport(PlainSaslServerProvider.MECHANISM, impersonationUser, null,
          null, new HashMap<String, String>(),
          new PlainSaslClientCallbackHandler(username, password), baseTransport);
    } catch (SaslException e) {
      throw new UnauthenticatedException(e.getMessage(), e);
    }
  }

  @Override
  public TTransportFactory getServerTransportFactory(String serverName) throws SaslException {
    return getServerTransportFactory(new Runnable() {
      @Override
      public void run() {}
    }, serverName);
  }

  @Override
  public TTransportFactory getServerTransportFactory(Runnable runnable, String serverName)
      throws SaslException {
    AuthType authType =
        Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    TSaslServerTransport.Factory saslFactory = new TSaslServerTransport.Factory();
    AuthenticationProvider provider =
        AuthenticationProvider.Factory.create(authType);
    saslFactory
        .addServerDefinition(PlainSaslServerProvider.MECHANISM, null, null,
            new HashMap<String, String>(), new PlainSaslServerCallbackHandler(provider, runnable));
    return saslFactory;
  }
}
