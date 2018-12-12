package alluxio.security.authentication;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.SaslAuthenticationServiceGrpc;
import alluxio.grpc.SaslMessage;
import alluxio.util.SecurityUtils;
import alluxio.grpc.GrpcChannelBuilder;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.NettyChannelBuilder;
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

/**
 * Used to authenticate with the target host. Used internally by {@link GrpcChannelBuilder}.
 */
public class ChannelBuilderAuthenticator {

  /** Whether to use mnarentSubject as authentication user. */
  protected boolean mUseSubject;
  /** Subject for authentication. */
  protected Subject mParentSubject;

  protected String mUserName;
  protected String mPassword;
  protected String mImpersonationUser;

  /** Target address. Expected to be serving {@link SaslAuthenticationServiceGrpc}. */
  protected InetSocketAddress mHostAddress;

  /** Authentication type to use with the target host. */
  protected AuthType mAuthType;

  /** Internal ID used to identify the channel that is being authenticated. */
  protected UUID mChannelId;

  /**
   * Creates {@link ChannelBuilderAuthenticator} instance.
   *
   * @param channelId channel Id
   * @param subject javax subject to use for authentication
   * @param hostAddress address for service host
   * @param authType authentication type
   */
  public ChannelBuilderAuthenticator(UUID channelId, Subject subject, InetSocketAddress hostAddress,
      AuthType authType) {
    mUseSubject = true;
    mChannelId = channelId;
    mParentSubject = subject;
    mHostAddress = hostAddress;
    mAuthType = authType;
  }

  /**
   * Creates {@link ChannelBuilderAuthenticator} instance.
   *
   * @param channelId channel id
   * @param userName user name
   * @param password user password
   * @param impersonationUser impersonation user
   * @param hostAddress address for service host
   * @param authType authentication type
   */
  public ChannelBuilderAuthenticator(UUID channelId, String userName, String password,
      String impersonationUser, InetSocketAddress hostAddress, AuthType authType) {
    mUseSubject = false;
    mChannelId = channelId;
    mUserName = userName;
    mPassword = password;
    mImpersonationUser = impersonationUser;
    mHostAddress = hostAddress;
    mAuthType = authType;
  }

  /**
   * Authenticates given {@link NettyChannelBuilder} instance. It attaches required interceptors
   * to the channel based on authentication type.
   *
   * @param channelBuilderToAuthenticate the channel builder for augmentation with interceptors
   * @return channel builder that is authenticated with the target host
   * @throws AuthenticationException
   */
  public NettyChannelBuilder authenticate(NettyChannelBuilder channelBuilderToAuthenticate)
      throws AuthenticationException {
    if (mAuthType == AuthType.NOSASL) {
      return channelBuilderToAuthenticate;
    }

    // Create a channel for talking with target host's authentication service.
    // TODO(ggezer) Consider pooling authentication channels per target.
    GrpcChannel authenticationChannel = GrpcChannelBuilder.forAddress(mHostAddress)
        .disableAuthentication().usePlaintext(true).build();
    try {
      // Create SaslClient for authentication based on provided credentials.
      SaslClient saslClient;
      if (mUseSubject) {
        saslClient =
            SaslParticipiantProvider.Factory.create(mAuthType).createSaslClient(mParentSubject);
      } else {
        saslClient = SaslParticipiantProvider.Factory.create(mAuthType).createSaslClient(mUserName,
            mPassword, mImpersonationUser);
      }

      // Create authentication scheme specific handshake handler.
      SaslHandshakeClientHandler handshakeClient =
          SaslHandshakeClientHandler.Factory.create(mAuthType, saslClient);
      // Create driver for driving sasl traffic from client side.
      SaslStreamClientDriver clientDriver = new SaslStreamClientDriver(handshakeClient);
      // Start authentication call with the service and update the client driver.
      StreamObserver<SaslMessage> requestObserver =
          SaslAuthenticationServiceGrpc.newStub(authenticationChannel).authenticate(clientDriver);
      clientDriver.setServerObserver(requestObserver);
      // Start authentication traffic with the target.
      clientDriver.start(mChannelId.toString());
      // Authentication succeeded!
      // Attach scheme specific interceptors to the channel.
      for (ClientInterceptor interceptor : getInterceptors(saslClient)) {
        channelBuilderToAuthenticate.intercept(interceptor);
      }
      return channelBuilderToAuthenticate;

    } catch (UnauthenticatedException e) {
      throw new AuthenticationException(e.getMessage(), e);
    } finally {
      // Close the authentication channel.
      authenticationChannel.shutdown();
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
        interceptorsList.add(new ChannelIdInjector(mChannelId));
        break;
      default:
        throw new RuntimeException(
            String.format("Authentication type:%s not supported", authType.name()));
    }
    return interceptorsList;
  }
}
