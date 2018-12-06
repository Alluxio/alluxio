package alluxio.security.authentication;

import alluxio.grpc.SaslMessage;
import io.grpc.stub.StreamObserver;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.util.UUID;

public class SaslStreamServerDriver implements StreamObserver<SaslMessage> {
  private StreamObserver<SaslMessage> mRequestObserver;
  private SaslHandshakeServerHandler mSaslHandshakeServerHandler;
  private AuthenticatedClientRegistry mClientRegistry;
  private UUID mClientId;

  public SaslStreamServerDriver(AuthenticatedClientRegistry clientRegistry) {
    mClientRegistry = clientRegistry;
  }

  public void setClientObserver(StreamObserver<SaslMessage> requestObserver) {
    mRequestObserver = requestObserver;
  }

  @Override
  public void onNext(SaslMessage saslMessage) {
    try {
      if(mSaslHandshakeServerHandler == null) {
        // New authentication request
        AuthType authType = AuthType.valueOf(saslMessage.getAuthenticationName());
        mClientId = UUID.fromString(saslMessage.getClientId());
        SaslServer saslServer = SaslParticipiantProvider.Factory.create(authType).getSaslServer("localhost");
        mSaslHandshakeServerHandler = SaslHandshakeServerHandler.Factory.create(authType, saslServer, mClientRegistry);
        // Unregister from registry if was authenticated before
        mClientRegistry.unregisterClient(mClientId);
      }

      mRequestObserver.onNext(mSaslHandshakeServerHandler.handleSaslMessage(saslMessage));
    } catch (SaslException e) {
      mRequestObserver.onError(e);
    }
  }

  @Override
  public void onError(Throwable throwable) {}

  @Override
  public void onCompleted() {
    mRequestObserver.onCompleted();
    mSaslHandshakeServerHandler.persistAuthenticationInfo(mClientId);
  }
}
