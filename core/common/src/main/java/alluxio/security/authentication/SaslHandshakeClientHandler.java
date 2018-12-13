package alluxio.security.authentication;

import alluxio.grpc.SaslMessage;
import alluxio.security.authentication.plain.SaslHandshakeClientHandlerPlain;

import javax.security.sasl.AuthenticationException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

/**
 * Interface for providing client-side handshake routines for a particular authentication scheme.
 */
public interface SaslHandshakeClientHandler {
  /**
   * Handles the given {@link SaslMessage} from the server.
   *
   * @param message server-side Sasl message to handle
   * @return client's answer. null if client is completed.
   * @throws SaslException
   */
  public SaslMessage handleSaslMessage(SaslMessage message) throws SaslException;

  /**
   * @param channelId channe for which the authentication is happening
   * @return the initial message for Sasl traffic to begin
   * @throws SaslException
   */
  public SaslMessage getInitialMessage(String channelId) throws SaslException;

  /**
   * Factory for {@link SaslHandshakeClientHandler}.
   */
  class Factory {

    // prevent instantiation
    private Factory() {}

    /**
     * @param authType authentication type to use
     * @return the generated {@link AuthenticationProvider}
     * @throws AuthenticationException when unsupported authentication type is used
     */
    public static SaslHandshakeClientHandler create(AuthType authType, SaslClient saslClient)
        throws AuthenticationException {
      switch (authType) {
        case SIMPLE:
        case CUSTOM:
          return new SaslHandshakeClientHandlerPlain(saslClient);
        default:
          throw new AuthenticationException("Unsupported AuthType: " + authType.getAuthName());
      }
    }
  }
}
