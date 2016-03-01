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

import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.io.IOException;
import java.security.Security;
import java.util.HashMap;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslException;

/**
 * Because the Java SunSASL provider doesn't support the server-side PLAIN mechanism. There is a new
 * provider {@link PlainSaslServerProvider} needed to support server-side PLAIN mechanism.
 * PlainSaslHelper is used to register this provider. It also provides methods to generate PLAIN
 * transport for server and client.
 */
@ThreadSafe
public final class PlainSaslUtils {
  static {
    Security.addProvider(new PlainSaslServerProvider());
  }

  /**
   * @return true if the provider was registered
   */
  public static boolean isPlainSaslProviderAdded() {
    return Security.getProvider(PlainSaslServerProvider.PROVIDER_NAME) != null;
  }

  /**
   * For server side, get a PLAIN mechanism {@link TTransportFactory}. A callback handler is hooked
   * for specific authentication methods.
   *
   * @param authType the authentication type
   * @param conf {@link Configuration}
   * @return a corresponding TTransportFactory, which is PLAIN mechanism
   * @throws SaslException if an {@link AuthenticationProvider} is not found
   */
  public static TTransportFactory getPlainServerTransportFactory(AuthType authType,
      Configuration conf) throws SaslException {
    TSaslServerTransport.Factory saslFactory = new TSaslServerTransport.Factory();
    AuthenticationProvider provider =
        AuthenticationProvider.Factory.create(authType, conf);
    saslFactory.addServerDefinition(PlainSaslServerProvider.MECHANISM, null, null,
        new HashMap<String, String>(), new PlainSaslServer.PlainServerCallbackHandler(provider));

    return saslFactory;
  }

  /**
   * Gets a PLAIN mechanism transport for client side.
   *
   * @param username User Name of PlainClient
   * @param password Password of PlainClient
   * @param wrappedTransport The original Transport
   * @return Wrapped transport with PLAIN mechanism
   * @throws SaslException if an AuthenticationProvider is not found
   */
  public static TTransport getPlainClientTransport(String username, String password,
      TTransport wrappedTransport) throws SaslException {
    return new TSaslClientTransport(PlainSaslServerProvider.MECHANISM, null, null, null,
        new HashMap<String, String>(), new PlainClientCallbackHandler(username, password),
        wrappedTransport);
  }

  /**
   * A client side callback to put application provided username/password into SASL transport.
   */
  public static class PlainClientCallbackHandler implements CallbackHandler {

    private final String mUserName;
    private final String mPassword;

    /**
     * Constructs a new client side callback.
     *
     * @param userName the name of the user
     * @param password the password
     */
    public PlainClientCallbackHandler(String userName, String password) {
      mUserName = userName;
      mPassword = password;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          NameCallback nameCallback = (NameCallback) callback;
          nameCallback.setName(mUserName);
        } else if (callback instanceof PasswordCallback) {
          PasswordCallback passCallback = (PasswordCallback) callback;
          passCallback.setPassword(mPassword == null ? null : mPassword.toCharArray());
        } else {
          Class<?> callbackClass = (callback == null) ? null : callback.getClass();
          throw new UnsupportedCallbackException(callback, callbackClass + " is unsupported.");
        }
      }
    }
  }
}
