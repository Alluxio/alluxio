package alluxio.security.authentication;

import alluxio.exception.status.UnauthenticatedException;

import javax.security.auth.Subject;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

public interface SaslParticipiantProvider {

  public SaslClient getSaslClient(Subject subject) throws UnauthenticatedException;

  public SaslClient getSaslClient(String username, String password, String impersonationUser)
      throws UnauthenticatedException;

  public SaslServer getSaslServer(String serverName) throws SaslException;

  public SaslServer getSaslServer(Runnable runnable, String serverName) throws SaslException;

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
