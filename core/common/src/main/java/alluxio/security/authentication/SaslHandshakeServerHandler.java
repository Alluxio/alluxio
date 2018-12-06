package alluxio.security.authentication;

import alluxio.grpc.SaslMessage;

import javax.security.sasl.AuthenticationException;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.util.UUID;

public interface SaslHandshakeServerHandler {
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
        public static SaslHandshakeServerHandler create(AuthType authType, SaslServer saslServer, AuthenticatedClientRegistry clientRegistry)
                throws AuthenticationException {
            switch (authType) {
                case SIMPLE:
                case CUSTOM:
                    return new SaslHandshakeServerHandlerPlain(saslServer, clientRegistry);
                default:
                    throw new AuthenticationException("Unsupported AuthType: " + authType.getAuthName());
            }
        }
    }

    public SaslMessage handleSaslMessage(SaslMessage message) throws SaslException;
    public void persistAuthenticationInfo(UUID clientId);
}
