package alluxio.security.authentication;

import alluxio.grpc.SaslMessage;
import alluxio.grpc.SaslMessageType;

import com.google.protobuf.ByteString;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.util.UUID;

public class SaslHandshakeServerHandlerPlain implements SaslHandshakeServerHandler {

  SaslServer mSaslServer;
  AuthenticatedClientRegistry mClientRegistry;

  public SaslHandshakeServerHandlerPlain(SaslServer saslServer,
      AuthenticatedClientRegistry clientRegistry) {
    mSaslServer = saslServer;
    mClientRegistry = clientRegistry;
  }

  @Override
  public SaslMessage handleSaslMessage(SaslMessage message) throws SaslException {
    switch (message.getMessageType()) {
      case CHALLANGE:
        byte[] saslResponse = mSaslServer.evaluateResponse(message.getMessage().toByteArray());
        SaslMessage.Builder response = SaslMessage.newBuilder();
        if (mSaslServer.isComplete()) {
          response.setMessageType(SaslMessageType.SUCCESS);
        } else {
          response.setMessageType(SaslMessageType.CHALLANGE);
        }
        if (saslResponse != null) {
          response.setMessage(ByteString.copyFrom(saslResponse));
        }
        return response.build();
      default:
        throw new SaslException("Server can't handle SUCCESS Sasl message");
    }
  }

  @Override
  public void persistAuthenticationInfo(UUID clientId) {
    AuthenticatedClient clientInfo = new AuthenticatedClient();
    clientInfo.setSaslServer(mSaslServer);
    clientInfo.setUserName(mSaslServer.getAuthorizationID());
    mClientRegistry.registerClient(clientId, clientInfo);
  }
}
