/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.grpc;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedChannelClientDriver;
import alluxio.security.authentication.ChannelIdInjector;
import alluxio.security.authentication.SaslClientHandler;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.auth.Subject;
import javax.security.sasl.SaslException;

/**
 * Used to gather gRPC level resources and indexes together.
 */
public class GrpcChannel extends Channel
    implements AutoCloseable {
  private static final long AUTH_TIMEOUT = Configuration.getMs(
      PropertyKey.NETWORK_CONNECTION_AUTH_TIMEOUT);

  private final GrpcChannelKey mChannelKey;
  private final ManagedChannel mManagedChannel;
  private final AtomicBoolean mChannelReleased = new AtomicBoolean(false);
  private final AtomicBoolean mChannelHealthy = new AtomicBoolean(true);
  private final AtomicReference<Channel> mChannel = new AtomicReference<>();
  private final AtomicReference<Optional<AuthenticatedChannelClientDriver>> mAuthDriver =
      new AtomicReference<>(Optional.empty());

  /**
   * Creates a new connection object.
   * @param channelKey gRPC channel key
   * @param managedChannel the underlying gRPC {@link ManagedChannel}
   */
  protected GrpcChannel(GrpcChannelKey channelKey, ManagedChannel managedChannel) {
    mChannelKey = Objects.requireNonNull(channelKey, "channelKey is null");
    mManagedChannel = Objects.requireNonNull(managedChannel, "managedChannel is null");
    mChannel.set(managedChannel);
  }

  /**
   * @return the hannel key that owns the connection
   */
  public GrpcChannelKey getChannelKey() {
    return mChannelKey;
  }

  /**
   * @return the channel
   */
  public Channel getChannel() {
    return mChannel.get();
  }

  @Override
  public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
      MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
    return mChannel.get().newCall(methodDescriptor, callOptions);
  }

  @Override
  public String authority() {
    return mChannel.get().authority();
  }

  protected void authenticate(AuthType authType, Subject subject, AlluxioConfiguration config)
      throws AlluxioStatusException {
    SaslClientHandler clientHandler;
    switch (authType) {
      case NOSASL:
        return;
      case SIMPLE:
      case CUSTOM:
        clientHandler =
            new alluxio.security.authentication.plain.SaslClientHandlerPlain(
                subject, config);
        break;
      default:

        throw new UnauthenticatedException(
            String.format("Channel authentication scheme not supported: %s", authType.name()));
    }
    AuthenticatedChannelClientDriver authDriver;
    try {
      // Create client-side driver for establishing authenticated channel with the target.
      authDriver = new AuthenticatedChannelClientDriver(clientHandler, mChannelKey);
    }
    catch (SaslException t) {
      AlluxioStatusException e = AlluxioStatusException.fromThrowable(t);
      // Build a pretty message for authentication failure.
      String message = String.format(
          "Channel authentication failed with code:%s. Channel: %s, AuthType: %s, Error: %s",
          e.getStatusCode().name(), mManagedChannel, authType, e);
      throw AlluxioStatusException
          .from(Status.fromCode(e.getStatusCode()).withDescription(message).withCause(t));
    }
    // Initialize client-server authentication drivers.
    SaslAuthenticationServiceGrpc.SaslAuthenticationServiceStub serverStub =
        SaslAuthenticationServiceGrpc.newStub(mManagedChannel);

    StreamObserver<SaslMessage> requestObserver = serverStub.authenticate(authDriver);
    authDriver.setServerObserver(requestObserver);
    // Start authentication with the target. (This is blocking.)
    authDriver.startAuthenticatedChannel(AUTH_TIMEOUT);
    mAuthDriver.set(Optional.of(authDriver));

    // Intercept authenticated channel with channel-id injector.
    intercept(new ChannelIdInjector(mChannelKey.getChannelId()));

    // An interceptor that is used to track server calls and invalidate the channel status. Upon
    // receiving Unauthenticated or Unavailable code from the server it invalidates the channel by
    // marking it unhealthy for channel owner to be able to detect and re-authenticate or re-create
    // the channel.
    intercept(new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
          MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next)
      {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
            next.newCall(method, callOptions)) {
          @Override
          public void start(Listener<RespT> responseListener, Metadata headers) {
            super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
                responseListener) {
              @Override
              public void onClose(io.grpc.Status status, Metadata trailers) {
                if (status == io.grpc.Status.UNAUTHENTICATED || status == Status.UNAVAILABLE) {
                  mChannelHealthy.set(false);
                }
                super.onClose(status, trailers);
              }
            }, headers);
          }
        };
      }
    });
  }

  /**
   * Registers interceptor to the channel.
   *
   * @param interceptor the gRPC client interceptor
   */
  public void intercept(ClientInterceptor interceptor) {
    Channel channel = mChannel.get();
    mChannel.compareAndSet(channel, ClientInterceptors.intercept(channel, interceptor));
  }

  /**
   * Shuts down the channel.
   *
   * Shutdown should be thread safe as it could be called concurrently due to:
   *  - Authentication long polling
   *  - gRPC messaging stream.
   */
  public void shutdown() {
    Optional<AuthenticatedChannelClientDriver> authDriver =
        mAuthDriver.getAndSet(Optional.empty());
    // Close authenticated session with server.
    authDriver.ifPresent(AuthenticatedChannelClientDriver::close);
    if (mChannelReleased.compareAndSet(false, true)) {
      try {
        close();
      } catch (Exception e) {
        // TODO(ggezer): Don't throw once stabilized, just trace.
        throw new RuntimeException("Failed to release the connection.", e);
      }
    }
  }

  /**
   * @return {@code true} if the channel has been shut down
   */
  public boolean isShutdown() {
    return mChannelReleased.get();
  }

  /**
   * @return {@code true} if channel is healthy
   */
  public boolean isHealthy() {
    return mChannelHealthy.get() && mAuthDriver.get()
        .map(AuthenticatedChannelClientDriver::isAuthenticated)
        .orElse(true);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof GrpcChannel)) {
      return false;
    }
    GrpcChannel otherConnection = (GrpcChannel) other;
    return Objects.equals(mChannelKey, otherConnection.mChannelKey)
        && Objects.equals(mManagedChannel, otherConnection.mManagedChannel)
        && Objects.equals(mChannel.get(), otherConnection.mChannel.get())
        && Objects.equals(mAuthDriver.get(), otherConnection.mAuthDriver.get());
  }

  @Override
  public int hashCode() {
    return Objects.hash(mChannelKey, mManagedChannel, mAuthDriver.get(), mChannel.get());
  }

  /**
   * Releases the connection to the pool.
   */
  @Override
  public void close() {
    // Release the connection back.
    GrpcChannelPool.INSTANCE.releaseConnection(mChannelKey);
  }
}
