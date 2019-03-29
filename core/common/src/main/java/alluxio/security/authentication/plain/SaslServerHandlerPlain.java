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

package alluxio.security.authentication.plain;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.security.authentication.AuthenticationProvider;
import alluxio.security.authentication.AuthenticationServer;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.SaslServerHandler;

import java.security.Security;
import java.util.HashMap;
import java.util.UUID;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * {@link SaslServerHandler} implementation for Plain/Custom schemes.
 */
public class SaslServerHandlerPlain implements SaslServerHandler {
  static {
    Security.addProvider(new PlainSaslServerProvider());
  }

  /** Underlying {@code SaslServer}. */
  private final SaslServer mSaslServer;

  /**
   * Creates {@link SaslServerHandler} for Plain/Custom.
   *
   * @param serverName server name
   * @param conf Alluxio configuration
   * @throws SaslException
   */
  public SaslServerHandlerPlain(String serverName, AlluxioConfiguration conf) throws SaslException {
    AuthType authType =
        conf.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    AuthenticationProvider provider = AuthenticationProvider.Factory.create(authType, conf);
    mSaslServer = Sasl.createSaslServer(PlainSaslServerProvider.MECHANISM, null, serverName,
        new HashMap<String, String>(), new PlainSaslServerCallbackHandler(provider, conf));
  }

  @Override
  public void authenticationCompleted(UUID channelId, AuthenticationServer authenticationServer) {
    authenticationServer.registerChannel(channelId,
        new AuthenticatedUserInfo(mSaslServer.getAuthorizationID()), mSaslServer);
  }

  @Override
  public void setAuthenticatedUserInfo(AuthenticatedUserInfo userinfo) {
    // Plain authentication only needs authorized user name which is available in completed
    // SaslServer instance.
  }

  @Override
  public SaslServer getSaslServer() {
    return mSaslServer;
  }
}
