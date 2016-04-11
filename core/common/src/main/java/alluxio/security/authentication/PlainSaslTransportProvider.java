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
import alluxio.security.LoginUser;

import com.google.common.base.Preconditions;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.Security;
import java.util.HashMap;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.sasl.SaslException;

/**
 * If authentication type is {@link AuthType#SIMPLE} or {@link AuthType#CUSTOM}, this is the
 * default transport provider which uses Sasl transport.
 */
@ThreadSafe
public final class PlainSaslTransportProvider implements TransportProvider {
  static {
    Security.addProvider(new PlainSaslServerProvider());
  }

  /** Timeout for socket in ms. */
  private int mSocketTimeoutMs;
  /** Configuration. */
  private Configuration mConfiguration;

  /**
   * Constructor for transport provider with {@link AuthType#SIMPLE} or {@link AuthType#CUSTOM}.
   *
   * @param conf Alluxio configuration
   */
  public PlainSaslTransportProvider(Configuration conf) {
    mConfiguration = Preconditions.checkNotNull(conf);
    mSocketTimeoutMs = conf.getInt(Constants.SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS);
  }

  @Override
  public TTransport getClientTransport(InetSocketAddress serverAddress) throws IOException {
    String username = LoginUser.get(mConfiguration).getName();
    String password = "noPassword";
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
   * @throws SaslException if an AuthenticationProvider is not found
   */
  public TTransport getClientTransport(String username, String password,
      InetSocketAddress serverAddress) throws SaslException {
    TTransport wrappedTransport =
        TransportProviderUtils.createThriftSocket(serverAddress, mSocketTimeoutMs);
    return new TSaslClientTransport(PlainSaslServerProvider.MECHANISM, null, null, null,
        new HashMap<String, String>(), new PlainSaslClientCallbackHandler(username, password),
        wrappedTransport);
  }

  @Override
  public TTransportFactory getServerTransportFactory() throws SaslException {
    AuthType authType =
        mConfiguration.getEnum(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    TSaslServerTransport.Factory saslFactory = new TSaslServerTransport.Factory();
    AuthenticationProvider provider =
        AuthenticationProvider.Factory.create(authType, mConfiguration);
    saslFactory
        .addServerDefinition(PlainSaslServerProvider.MECHANISM, null, null,
            new HashMap<String, String>(), new PlainSaslServerCallbackHandler(provider));
    return saslFactory;
  }
}
