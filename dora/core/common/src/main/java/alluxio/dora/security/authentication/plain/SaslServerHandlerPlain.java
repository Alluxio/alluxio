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

package alluxio.dora.security.authentication.plain;

import alluxio.dora.conf.AlluxioConfiguration;
import alluxio.dora.conf.PropertyKey;
import alluxio.dora.security.authentication.AbstractSaslServerHandler;
import alluxio.dora.security.authentication.AuthType;
import alluxio.dora.security.authentication.AuthenticatedUserInfo;
import alluxio.dora.security.authentication.AuthenticationProvider;
import alluxio.dora.security.authentication.ImpersonationAuthenticator;
import alluxio.dora.security.authentication.SaslServerHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Security;
import java.util.HashMap;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

/**
 * {@link SaslServerHandler} implementation for Plain/Custom schemes.
 */
public class SaslServerHandlerPlain extends AbstractSaslServerHandler {
  private static final Logger LOG = LoggerFactory.getLogger(SaslServerHandlerPlain.class);

  static {
    Security.addProvider(new PlainSaslServerProvider());
  }

  /**
   * Creates {@link SaslServerHandler} for Plain/Custom.
   *
   * @param serverName server name
   * @param conf Alluxio configuration
   * @param authenticator the impersonation authenticator
   * @throws SaslException
   */
  public SaslServerHandlerPlain(String serverName, AlluxioConfiguration conf,
      ImpersonationAuthenticator authenticator) throws SaslException {
    AuthType authType =
        conf.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    AuthenticationProvider provider = AuthenticationProvider.Factory.create(authType, conf);
    mSaslServer = Sasl.createSaslServer(PlainSaslServerProvider.MECHANISM, null, serverName,
        new HashMap<String, String>(), new PlainSaslServerCallbackHandler(provider, authenticator));
  }

  @Override
  public void setAuthenticatedUserInfo(AuthenticatedUserInfo userinfo) {
    // Plain authentication only needs authorized user name which is available in completed
    // SaslServer instance.
  }

  @Override
  public AuthenticatedUserInfo getAuthenticatedUserInfo() {
    return new AuthenticatedUserInfo(mSaslServer.getAuthorizationID());
  }
}
