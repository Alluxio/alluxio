package alluxio.security.authentication;

import alluxio.grpc.SaslMessage;

import javax.security.sasl.AuthenticationException;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.util.UUID;

/**
 * Interface for providing server-side handshake routines for a particular authentication scheme.
 */
public interface SaslHandshakeServerHandler {
  /**
   * Handles given {@link SaslMessage} from the client.
   *
   * @param message client Sasl message
   * @return server's response to given client message
   * @throws SaslException
   */
  public SaslMessage handleSaslMessage(SaslMessage message) throws SaslException;

  /**
   * Factory for {@link SaslHandshakeServerHandler}.
   */
  class Factory {

    // prevent instantiation
    private Factory() {}

    /**
     * @param authType authentication type to use
     * @return the generated {@link AuthenticationProvider}
     * @throws AuthenticationException when unsupported authentication type is used
     */
    public static SaslHandshakeServerHandler create(AuthType authType, SaslServer saslServer)
        throws AuthenticationException {
      switch (authType) {
        case SIMPLE:
        case CUSTOM:
          return new SaslHandshakeServerHandlerPlain(saslServer);
        default:
          throw new AuthenticationException("Unsupported AuthType: " + authType.getAuthName());
      }
    }
  }
}
