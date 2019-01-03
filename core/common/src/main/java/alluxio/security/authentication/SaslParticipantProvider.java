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

import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.authentication.plain.SaslParticipantProviderPlain;

import javax.security.auth.Subject;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * Interface for providing Sasl participants (client/server) for particular authentication scheme.
 */
public interface SaslParticipantProvider {

  /**
   * Creates {@link SaslClient} for given {@link Subject}.
   *
   * @param subject subject
   * @return created {@link SaslClient}
   * @throws UnauthenticatedException
   */
  public SaslClient createSaslClient(Subject subject) throws UnauthenticatedException;

  /**
   * Creates {@link SaslClient} for given authentication info.
   *
   * @param username user name
   * @param password password
   * @param impersonationUser impersonation user
   * @return created {@link SaslClient}
   * @throws UnauthenticatedException
   */
  public SaslClient createSaslClient(String username, String password, String impersonationUser)
      throws UnauthenticatedException;

  /**
   * Creates {@link SaslServer}.
   *
   * @param serverName server name
   * @return created {@link SaslServer}
   * @throws SaslException
   */
  public SaslServer createSaslServer(String serverName) throws SaslException;

  /**
   * Creates {@link SaslServer}.
   *
   * @param runnable callback for after authentication
   * @param serverName server name
   * @return created {@link SaslServer}
   * @throws SaslException
   */
  public SaslServer createSaslServer(Runnable runnable, String serverName) throws SaslException;

  /**
   * Factory for {@link SaslParticipantProvider}.
   */
  class Factory {

    // prevent instantiation
    private Factory() {}

    /**
     * @param authType authentication type to use
     * @return the generated {@link AuthenticationProvider}
     * @throws UnauthenticatedException when unsupported authentication type is used
     */
    public static SaslParticipantProvider create(AuthType authType)
        throws UnauthenticatedException {
      switch (authType) {
        case SIMPLE:
        case CUSTOM:
          return new SaslParticipantProviderPlain();
        default:
          throw new UnauthenticatedException("Unsupported AuthType: " + authType.getAuthName());
      }
    }
  }
}
