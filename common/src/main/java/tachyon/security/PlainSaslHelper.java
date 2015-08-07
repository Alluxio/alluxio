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

package tachyon.security;

import java.util.HashMap;

import javax.security.sasl.SaslException;
import java.security.Security;

import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransportFactory;

import tachyon.conf.TachyonConf;
import tachyon.security.authentication.AuthenticationFactory.AuthType;
import tachyon.security.authentication.AuthenticationProvider;
import tachyon.security.authentication.AuthenticationProviderFactory;

/**
 * Because the Java SunSASL provider doesn't support the server-side PLAIN mechanism.
 * There is a new provider {@link PlainSaslServerProvider} needed to support server-side
 * PLAIN mechanism.
 * PlainSaslHelper is used to register this provider.
 * It also provides methods to generate PLAIN transport for server and client.
 */
public class PlainSaslHelper {
  static {
    Security.addProvider(new PlainSaslServerProvider());
  }

  /**
   * @param name the name of the provider
   * @return true if the provider was registered
   */
  public static boolean isPlainSaslProviderAdded() {
    return Security.getProvider(PlainSaslServerProvider.PROVIDER) != null;
  }

  /**
   * For server side, get a PLAIN mechanism TTransportFactory. A callback handler is hooked for
   * specific authentication methods.
   * @param authType the authentication type
   * @param conf TachyonConf
   * @return a corresponding TTransportFactory, which is PLAIN mechanism
   * @throws SaslException if an AuthenticationProvider is not found
   */
  public static TTransportFactory getPlainServerTransportFactory(AuthType authType,
      TachyonConf conf) throws SaslException {
    TSaslServerTransport.Factory saslFactory = new TSaslServerTransport.Factory();
    AuthenticationProvider provider = AuthenticationProviderFactory
        .getAuthenticationProvider(authType, conf);
    saslFactory.addServerDefinition("PLAIN", null, null, new HashMap<String, String>(),
        new PlainSaslServer.PlainServerCallbackHandler(provider));

    return saslFactory;
  }

  // TODO: get client side PLAIN TTransport
//  public static TTransport getPlainClientTransport(String username, String password,
//      TTransport wrappedTransport) throws SaslException {}
}
