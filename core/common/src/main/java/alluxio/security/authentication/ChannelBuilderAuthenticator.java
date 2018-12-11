package alluxio.security.authentication;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.AlluxioSaslClientServiceGrpc;
import alluxio.grpc.SaslMessage;
import alluxio.util.SecurityUtils;
import alluxio.grpc.GrpcChannelBuilder;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import javax.security.auth.Subject;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.SaslClient;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ChannelBuilderAuthenticator {

  protected Subject mParentSubject;
  protected InetSocketAddress mHostAddress;
  protected AuthType mAuthType;
  protected UUID mClientId;

  public ChannelBuilderAuthenticator(UUID clientId, Subject subject, InetSocketAddress hostAddress,
      AuthType authType) {
    mParentSubject = subject;
    mHostAddress = hostAddress;
    mAuthType = authType;
    mClientId = clientId;
  }

  /**
   * Authenticates and augments given {@link GrpcChannelBuilder} instance.
   *
   * @param channelBuilderToAuthenticate the channel builder for augmentation with interceptors.
   * @return channel builder
   * @throws AuthenticationException
   */
  public ManagedChannelBuilder authenticate(ManagedChannelBuilder channelBuilderToAuthenticate)
      throws AuthenticationException {
    if (mAuthType == AuthType.NOSASL) {
      return channelBuilderToAuthenticate;
    }

    ManagedChannel authenticationChannel = ManagedChannelBuilder
        .forAddress(mHostAddress.getHostName(), mHostAddress.getPort()).usePlaintext(true).build();
    try {
      SaslClient saslClient =
          SaslParticipiantProvider.Factory.create(mAuthType).getSaslClient(mParentSubject);
      SaslHandshakeClientHandler handshakeClient =
          SaslHandshakeClientHandler.Factory.create(mAuthType, saslClient);

      SaslStreamClientDriver clientDriver = new SaslStreamClientDriver(handshakeClient);

      StreamObserver<SaslMessage> requestObserver =
          AlluxioSaslClientServiceGrpc.newStub(authenticationChannel).authenticate(clientDriver);
      clientDriver.setServerObserver(requestObserver);
      // Start authentication with the target server
      clientDriver.start(mClientId.toString());

      for (ClientInterceptor interceptor : getInterceptors(saslClient)) {
        channelBuilderToAuthenticate.intercept(interceptor);
      }

      return channelBuilderToAuthenticate;

    } catch (UnauthenticatedException e) {
      throw new AuthenticationException(e.getMessage(), e);
    } finally {
      authenticationChannel.shutdown();
      while (!authenticationChannel.isTerminated())
        try {
          authenticationChannel.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {

        }
    }
  }

  /**
   * @param saslClient the Sasl client object that have been used for authentication
   * @return the list of interceptors that will be attached to the newly authenticated channel.
   */
  private List<ClientInterceptor> getInterceptors(SaslClient saslClient) {
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
