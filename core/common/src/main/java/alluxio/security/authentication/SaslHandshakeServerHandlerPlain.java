package alluxio.security.authentication;

import alluxio.grpc.SaslMessage;
import alluxio.grpc.SaslMessageType;

import com.google.protobuf.ByteString;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * Implementation of {@link SaslHandshakeServerHandler} for plain authentication.
 */
public class SaslHandshakeServerHandlerPlain implements SaslHandshakeServerHandler {

  /** SaslServer that will be used. */
  SaslServer mSaslServer;

  /**
   * Creates {@link SaslHandshakeServerHandlerPlain} with given {@link SaslServer}.
   * 
   * @param saslServer sasl server
   */
  public SaslHandshakeServerHandlerPlain(SaslServer saslServer) {
    mSaslServer = saslServer;
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
        throw new SaslException(
            "Server can't process Sasl message type:" + message.getMessageType().name());
    }
  }
}
