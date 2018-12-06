package alluxio.security.authentication;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.AlluxioSaslClientServiceGrpc;
import alluxio.grpc.SaslMessage;
import alluxio.util.grpc.GrpcChannel;
import io.grpc.stub.StreamObserver;

import javax.security.auth.Subject;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.SaslClient;
import java.util.UUID;

public class AuthenticationClient {

  protected Subject mParentSubject;
  protected GrpcChannel mServerChannel;
  protected AuthType mAuthType;
  protected UUID mClientId;


  public AuthenticationClient(UUID clientId, Subject subject, GrpcChannel serverChannel) {
    this(clientId, subject, serverChannel,
        Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class));
  }

  public AuthenticationClient(UUID clientId, Subject subject, GrpcChannel serverChannel,
      AuthType authType) {
    mParentSubject = subject;
    mServerChannel = serverChannel;
    mAuthType = authType;
    mClientId = clientId;
  }

  public void authenticate() throws AuthenticationException {
    if(mAuthType == AuthType.NOSASL){
      return;
    }

    try {
      SaslClient client =
          SaslParticipiantProvider.Factory.create(mAuthType).getSaslClient(mParentSubject);
      SaslHandshakeClientHandler handshakeClient = SaslHandshakeClientHandler.Factory.create(mAuthType, client);

      SaslStreamClientDriver clientDriver = new SaslStreamClientDriver(handshakeClient);

      StreamObserver<SaslMessage> requestObserver =
          AlluxioSaslClientServiceGrpc.newStub(mServerChannel).authenticate(clientDriver);
      clientDriver.setServerObserver(requestObserver);

      clientDriver.start(mClientId.toString());

    } catch (UnauthenticatedException e) {
      throw new AuthenticationException(e.getMessage(), e);
    }
  }
}
