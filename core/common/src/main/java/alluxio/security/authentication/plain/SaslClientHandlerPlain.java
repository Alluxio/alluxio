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
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.ChannelAuthenticationScheme;
import alluxio.security.User;
import alluxio.security.authentication.SaslClientHandler;
import alluxio.security.authentication.AuthenticationUserUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.util.HashMap;
import java.util.Set;

/**
 * {@link SaslClientHandler} implementation for Plain/Custom schemes.
 */
public class SaslClientHandlerPlain implements SaslClientHandler {
  private static final Logger LOG = LoggerFactory.getLogger(SaslClientHandlerPlain.class);

  /** Underlying SaslClient. */
  private final SaslClient mSaslClient;

  /**
   * Creates {@link SaslClientHandler} instance for Plain/Custom.
   *
   * @param subject client subject
   * @param conf Alluxio configuration
   * @throws UnauthenticatedException
   */
  public SaslClientHandlerPlain(Subject subject, AlluxioConfiguration conf)
      throws UnauthenticatedException {
    if (subject == null) {
      throw new UnauthenticatedException("client subject not provided");
    }
    String connectionUser = null;
    String password = "noPassword";

    Set<User> user = subject.getPrincipals(User.class);
    if (user != null && !user.isEmpty()) {
      connectionUser = user.iterator().next().getName();
    }

    // Determine the impersonation user
    String impersonationUser = AuthenticationUserUtils.getImpersonationUser(subject, conf);

    mSaslClient = createSaslClient(connectionUser, password, impersonationUser);
  }

  /**
   * Creates {@link SaslClientHandler} instance for Plain/Custom.
   *
   * @param username user name
   * @param password password
   * @param impersonationUser impersonation user
   * @throws UnauthenticatedException
   */
  public SaslClientHandlerPlain(String username, String password, String impersonationUser)
      throws UnauthenticatedException {
    mSaslClient = createSaslClient(username, password, impersonationUser);
  }

  private SaslClient createSaslClient(String username, String password, String impersonationUser)
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
  public ChannelAuthenticationScheme getClientScheme() {
    return ChannelAuthenticationScheme.SIMPLE;
  }

  @Override
  public SaslClient getSaslClient() {
    return mSaslClient;
  }

  @Override
  public void close() {
    if (mSaslClient != null) {
      try {
        mSaslClient.dispose();
      } catch (SaslException exc) {
        LOG.debug("Failed to close SaslClient.", exc);
      }
    }
  }
}
