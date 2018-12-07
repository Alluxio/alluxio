package alluxio.security.authentication;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.AlluxioSaslClientServiceGrpc;
import alluxio.grpc.SaslMessage;
import alluxio.util.SecurityUtils;
import alluxio.util.grpc.GrpcChannel;
import alluxio.util.grpc.GrpcChannelBuilder;
import io.grpc.ClientInterceptor;
import io.grpc.ServerInterceptor;
import io.grpc.stub.StreamObserver;

import javax.security.auth.Subject;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.SaslClient;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class AuthenticatedChannelBuilder {

  protected Subject mParentSubject;
  protected InetSocketAddress mHostAddress;
  protected GrpcChannelBuilder mChannelBuilder;
  protected AuthType mAuthType;
  protected UUID mClientId;


  public AuthenticatedChannelBuilder(UUID clientId, Subject subject,
      InetSocketAddress serverAddress) {
    this(clientId, subject, serverAddress,
        Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class));
  }

  public AuthenticatedChannelBuilder(UUID clientId, Subject subject, InetSocketAddress hostAddress,
      AuthType authType) {
    mParentSubject = subject;
    mHostAddress = hostAddress;
    mChannelBuilder = GrpcChannelBuilder.forAddress(mHostAddress);
    mAuthType = authType;
    mClientId = clientId;
  }

  public GrpcChannelBuilder create() throws AuthenticationException {
    if (mAuthType == AuthType.NOSASL) {
      return mChannelBuilder;
    }

    GrpcChannel authenticationChannel =
        GrpcChannelBuilder.forAddress(mHostAddress).usePlaintext(true).build();
    try {
      SaslClient client =
          SaslParticipiantProvider.Factory.create(mAuthType).getSaslClient(mParentSubject);
      SaslHandshakeClientHandler handshakeClient =
          SaslHandshakeClientHandler.Factory.create(mAuthType, client);

      SaslStreamClientDriver clientDriver = new SaslStreamClientDriver(handshakeClient);

      StreamObserver<SaslMessage> requestObserver =
          AlluxioSaslClientServiceGrpc.newStub(authenticationChannel).authenticate(clientDriver);
      clientDriver.setServerObserver(requestObserver);
      // Start authentication with the target server
      clientDriver.start(mClientId.toString());

      for (ClientInterceptor interceptor : getInterceptors()) {
        mChannelBuilder.intercept(interceptor);
      }

      return mChannelBuilder;

    } catch (UnauthenticatedException e) {
      throw new AuthenticationException(e.getMessage(), e);
    } finally {
      authenticationChannel.shutdown();
    }
  }

  public List<ClientInterceptor> getInterceptors() {
    if (!SecurityUtils.isSecurityEnabled()) {
      return Collections.emptyList();
    }
    List<ClientInterceptor> interceptorsList = new ArrayList<>();
    AuthType authType =
        Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    switch (authType) {
      case SIMPLE:
      case CUSTOM:
        interceptorsList.add(new ClientIdInjector(mClientId));
        break;
      default:
        throw new RuntimeException(
            String.format("Authentication type:%s not supported", authType.name()));
    }
    return interceptorsList;
  }
}
