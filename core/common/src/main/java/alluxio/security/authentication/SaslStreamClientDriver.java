package alluxio.security.authentication;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.grpc.SaslMessage;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import javax.security.sasl.AuthenticationException;
import javax.security.sasl.SaslException;

public class SaslStreamClientDriver implements StreamObserver<SaslMessage> {
  private StreamObserver<SaslMessage> mRequestObserver;
  private SaslHandshakeClientHandler mSaslHandshakeClientHandler;
  private SettableFuture<Boolean> mAuthenticated;

  public SaslStreamClientDriver(SaslHandshakeClientHandler handshakeClient) {
    mSaslHandshakeClientHandler = handshakeClient;
    mAuthenticated = SettableFuture.create();
  }

  public void setServerObserver(StreamObserver<SaslMessage> requestObserver) {
    mRequestObserver = requestObserver;
  }

  @Override
  public void onNext(SaslMessage saslMessage) {
    try {
      SaslMessage response = mSaslHandshakeClientHandler.handleSaslMessage(saslMessage);
      if (response == null) {
        mRequestObserver.onCompleted();
      } else {
        mRequestObserver.onNext(response);
      }
    } catch (SaslException e) {
      mAuthenticated.setException(e);
      mRequestObserver
          .onError(Status.fromCode(Status.Code.UNAUTHENTICATED).withCause(e).asException());
    }
  }

  @Override
  public void onError(Throwable throwable) {
    mAuthenticated.setException(throwable);
  }

  @Override
  public void onCompleted() {
    mAuthenticated.set(true);
  }

  public void start(String clientId) throws AuthenticationException {
    try {
      mRequestObserver.onNext(mSaslHandshakeClientHandler.getInitialMessage(clientId));
      mAuthenticated.get();
    } catch (Exception e) {
      throw new AuthenticationException(e.getMessage(), e);
    }
  }
}
