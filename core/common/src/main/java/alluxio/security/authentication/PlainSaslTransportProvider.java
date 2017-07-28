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
import alluxio.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
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

  /** Timeout for socket in ms. */
  private int mSocketTimeoutMs;

  /**
   * Constructor for transport provider with {@link AuthType#SIMPLE} or {@link AuthType#CUSTOM}.
   */
  public PlainSaslTransportProvider() {
    mSocketTimeoutMs =
        (int) Configuration.getMs(PropertyKey.SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS);
  }

  @Override
  public TTransport getClientTransport(InetSocketAddress serverAddress)
      throws UnauthenticatedException {
    String username = LoginUser.get().getName();
    String password = "noPassword";
    return getClientTransport(username, password, serverAddress);
  }

  @Override
  public TTransport getClientTransport(Subject subject, InetSocketAddress serverAddress)
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
    return getClientTransport(username, password, serverAddress);
  }

  // TODO(binfan): make this private and use whitebox to access this method in test
  /**
   * Gets a PLAIN mechanism transport for client side.
   *
   * @param username User Name of PlainClient
   * @param password Password of PlainClient
   * @param serverAddress Address of the server
   * @return Wrapped transport with PLAIN mechanism
   */
  public TTransport getClientTransport(String username, String password,
      InetSocketAddress serverAddress) throws UnauthenticatedException {
    TTransport wrappedTransport =
        TransportProviderUtils.createThriftSocket(serverAddress, mSocketTimeoutMs);
    try {
      return new TSaslClientTransport(PlainSaslServerProvider.MECHANISM, null, null, null,
          new HashMap<String, String>(), new PlainSaslClientCallbackHandler(username, password),
          wrappedTransport);
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
