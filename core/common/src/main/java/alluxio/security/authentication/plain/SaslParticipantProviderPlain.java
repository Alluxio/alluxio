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
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.LoginUser;
import alluxio.security.User;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticationProvider;
import alluxio.security.authentication.SaslParticipantProvider;
import alluxio.security.authentication.SaslParticipantProviderUtils;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.security.Security;
import java.util.HashMap;
import java.util.Set;

/**
 * Implementation of {@link SaslParticipantProvider} for plain authentication.
 */
public class SaslParticipantProviderPlain implements SaslParticipantProvider {
  static {
    // Register Sasl server implementation for plain mechanism.
    Security.addProvider(new PlainSaslServerProvider());
  }

  @Override
  public SaslClient createSaslClient(Subject subject, AlluxioConfiguration conf)
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
      username = LoginUser.get(conf).getName();
    }

    // Determine the impersonation user
    String impersonationUser = SaslParticipantProviderUtils.getImpersonationUser(subject, conf);

    if (impersonationUser != null
        && conf.isSet(PropertyKey.SECURITY_LOGIN_IMPERSONATION_USERNAME)
        && Constants.IMPERSONATION_HDFS_USER
            .equals(conf.get(PropertyKey.SECURITY_LOGIN_IMPERSONATION_USERNAME))) {
      // If impersonation is configured to use the HDFS user, the connection user should
      // be not be the HDFS user, but the LoginUser.
      // If the HDFS user is really supposed to be the connection user, that can be achieved by
      // not enabling impersonation for the client.
      username = LoginUser.get(conf).getName();
    }
    return createSaslClient(username, password, impersonationUser);
  }

  @Override
  public SaslClient createSaslClient(String username, String password, String impersonationUser)
      throws UnauthenticatedException {
    try {
      return Sasl.createSaslClient(new String[] {PlainSaslServerProvider.MECHANISM},
          impersonationUser, null, null, new HashMap<String, String>(),
          new PlainSaslClientCallbackHandler(username, password));
    } catch (SaslException e) {
      throw new UnauthenticatedException(e.getMessage(), e);
    }
  }

  @Override
  public SaslServer createSaslServer(String serverName, AlluxioConfiguration conf)
      throws SaslException {
    return createSaslServer(new Runnable() {
      @Override
      public void run() {}
    }, serverName, conf);
  }

  @Override
  public SaslServer createSaslServer(Runnable runnable, String serverName,
      AlluxioConfiguration conf) throws SaslException {
    AuthType authType =
        conf.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    AuthenticationProvider provider = AuthenticationProvider.Factory.create(authType, conf);
    return Sasl.createSaslServer(PlainSaslServerProvider.MECHANISM, null, serverName,
        new HashMap<String, String>(), new PlainSaslServerCallbackHandler(provider, runnable,
            conf));
  }
}
