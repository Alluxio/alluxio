package alluxio.security.authentication;

import alluxio.grpc.SaslMessage;

import javax.security.sasl.AuthenticationException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

public interface SaslHandshakeClientHandler {
  public SaslMessage handleSaslMessage(SaslMessage message) throws SaslException;

  public SaslMessage getInitialMessage(String clientId) throws SaslException;

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
