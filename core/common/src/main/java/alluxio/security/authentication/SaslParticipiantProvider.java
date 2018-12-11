package alluxio.security.authentication;

import alluxio.exception.status.UnauthenticatedException;

import javax.security.auth.Subject;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * Interface for providing Sasl participiants (client/server) for particular authentication scheme.
 */
public interface SaslParticipiantProvider {

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
   * Factory for {@link SaslParticipiantProvider}.
   */
  class Factory {

    // prevent instantiation
    private Factory() {}

    /**
     * @param authType authentication type to use
     * @return the generated {@link AuthenticationProvider}
     * @throws AuthenticationException when unsupported authentication type is used
     */
    public static SaslParticipiantProvider create(AuthType authType)
        throws AuthenticationException {
      switch (authType) {
        case SIMPLE:
        case CUSTOM:
          return new SaslParticipiantProviderPlain();
        default:
          throw new AuthenticationException("Unsupported AuthType: " + authType.getAuthName());
      }
    }
  }
}
