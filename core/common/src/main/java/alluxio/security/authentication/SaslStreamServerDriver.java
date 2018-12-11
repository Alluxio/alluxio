package alluxio.security.authentication;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.grpc.SaslMessage;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.util.UUID;

public class SaslStreamServerDriver implements StreamObserver<SaslMessage> {
  private StreamObserver<SaslMessage> mRequestObserver;
  private SaslHandshakeServerHandler mSaslHandshakeServerHandler;
  private AuthenticationServer mAuthenticationServer;
  private UUID mClientId;
  private SaslServer mSaslServer;

  public SaslStreamServerDriver(AuthenticationServer authenticationServer) {
    mAuthenticationServer = authenticationServer;
  }

  public void setClientObserver(StreamObserver<SaslMessage> requestObserver) {
    mRequestObserver = requestObserver;
  }

  @Override
  public void onNext(SaslMessage saslMessage) {
    try {
      if (mSaslHandshakeServerHandler == null) {
        // New authentication request
        AuthType authType = AuthType.valueOf(saslMessage.getAuthenticationName());
        mClientId = UUID.fromString(saslMessage.getClientId());
        // TODO(ggezer) wire server name
        mSaslServer = SaslParticipiantProvider.Factory.create(authType).getSaslServer("localhost");
        mSaslHandshakeServerHandler =
            SaslHandshakeServerHandler.Factory.create(authType, mSaslServer);
        // Unregister from registry if was authenticated before
        mAuthenticationServer.unregisterClient(mClientId);
      }

      mRequestObserver.onNext(mSaslHandshakeServerHandler.handleSaslMessage(saslMessage));
    } catch (SaslException e) {
      mRequestObserver
          .onError(Status.fromCode(Status.Code.UNAUTHENTICATED).withCause(e).asException());
    }
  }

  @Override
  public void onError(Throwable throwable) {}

  @Override
  public void onCompleted() {
    mAuthenticationServer.registerClient(mClientId, mSaslServer.getAuthorizationID(), mSaslServer);
    mRequestObserver.onCompleted();
  }
}
