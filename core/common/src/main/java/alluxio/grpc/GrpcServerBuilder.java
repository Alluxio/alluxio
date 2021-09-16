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

import alluxio.annotation.SuppressFBWarnings;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.authentication.AuthenticatedUserInjector;
import alluxio.security.authentication.AuthenticationServer;
import alluxio.security.authentication.DefaultAuthenticationServer;
import alluxio.security.user.UserState;
import alluxio.util.SecurityUtils;
import alluxio.util.network.tls.SslContextProvider;

import com.google.common.io.Closer;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.handler.ssl.SslContext;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * Provides authenticated gRPC server creation.
 */
public final class GrpcServerBuilder {

  /** Internal netty builder. */
  private NettyServerBuilder mNettyServerBuilder;
  /** Set of services that this server has. */
  private Set<ServiceType> mServices;
  /** Authentication server instance that will be used by this server. */
  private AuthenticationServer mAuthenticationServer;
  /** Used to register closers that needs to be called during server shut-down. */
  private Closer mCloser = Closer.create();
  /** Alluxio configuration.  */
  private AlluxioConfiguration mConfiguration;

  @SuppressFBWarnings(value = "URF_UNREAD_FIELD")
  private UserState mUserState;

  private GrpcServerBuilder(GrpcServerAddress serverAddress,
      AuthenticationServer authenticationServer, AlluxioConfiguration conf, UserState userState) {
    mAuthenticationServer = authenticationServer;
    mNettyServerBuilder = NettyServerBuilder.forAddress(serverAddress.getSocketAddress());
    mServices = new HashSet<>();
    mConfiguration = conf;
    mUserState = userState;

    if (conf.getBoolean(alluxio.conf.PropertyKey.NETWORK_TLS_ENABLED)) {
      sslContext(SslContextProvider.Factory.create(mConfiguration).getServerSSLContext());
    }

    if (SecurityUtils.isAuthenticationEnabled(mConfiguration)) {
      if (mAuthenticationServer == null) {
        mAuthenticationServer =
            new DefaultAuthenticationServer(serverAddress.getHostName(), mConfiguration);
      }
      addService(new GrpcService(mAuthenticationServer).disableAuthentication()
          .withCloseable(mAuthenticationServer));
    }
  }

  /**
   * Create an new instance of {@link GrpcServerBuilder} with authentication support.
   *
   * @param serverAddress server address
   * @param conf Alluxio configuration
   * @param userState the user state
   * @return a new instance of {@link GrpcServerBuilder}
   */
  public static GrpcServerBuilder forAddress(GrpcServerAddress serverAddress,
      AlluxioConfiguration conf, UserState userState) {
    return new GrpcServerBuilder(serverAddress, null, conf, userState);
  }

  /**
   * Create an new instance of {@link GrpcServerBuilder} with authentication support.
   *
   * @param serverAddress server address
   * @param authenticationServer the authentication server to use
   * @param conf the Alluxio configuration
   * @param userState the user state
   * @return a new instance of {@link GrpcServerBuilder}
   */
  public static GrpcServerBuilder forAddress(GrpcServerAddress serverAddress,
      AuthenticationServer authenticationServer, AlluxioConfiguration conf, UserState userState) {
    return new GrpcServerBuilder(serverAddress, authenticationServer, conf, userState);
  }

  /**
   * Set the executor for this server.
   *
   * @param executor the executor
   * @return an updated instance of this {@link GrpcServerBuilder}
   */
  public GrpcServerBuilder executor(@Nullable Executor executor) {
    mNettyServerBuilder = mNettyServerBuilder.executor(executor);
    return this;
  }

  /**
   * Sets flow control window.
   *
   * @param flowControlWindow the HTTP2 flow control window
   * @return an updated instance of this {@link GrpcServerBuilder}
   */
  public GrpcServerBuilder flowControlWindow(int flowControlWindow) {
    mNettyServerBuilder = mNettyServerBuilder.flowControlWindow(flowControlWindow);
    return this;
  }

  /**
   * Sets the keep alive time.
   *
   * @param keepAliveTime the time to wait after idle before pinging client
   * @param timeUnit unit of the time
   * @return an updated instance of this {@link GrpcServerBuilder}
   */
  public GrpcServerBuilder keepAliveTime(long keepAliveTime, TimeUnit timeUnit) {
    mNettyServerBuilder = mNettyServerBuilder.keepAliveTime(keepAliveTime, timeUnit);
    return this;
  }

  /**
   * Sets the keep alive timeout.
   *
   * @param keepAliveTimeout time to wait after pinging client before closing the connection
   * @param timeUnit unit of the timeout
   * @return an updated instance of this {@link GrpcServerBuilder}
   */
  public GrpcServerBuilder keepAliveTimeout(long keepAliveTimeout, TimeUnit timeUnit) {
    mNettyServerBuilder = mNettyServerBuilder.keepAliveTimeout(keepAliveTimeout, timeUnit);
    return this;
  }

  /**
   * Sets the netty channel type.
   *
   * @param channelType the netty channel type for the server
   * @return an updated instance of this {@link GrpcServerBuilder}
   */
  public GrpcServerBuilder channelType(Class<? extends ServerChannel> channelType) {
    mNettyServerBuilder = mNettyServerBuilder.channelType(channelType);
    return this;
  }

  /**
   * Sets a netty channel option.
   *
   * @param <T> channel option type
   * @param option the option to be set
   * @param value the new value
   * @return an updated instance of this {@link GrpcServerBuilder}
   */
  public <T> GrpcServerBuilder withChildOption(ChannelOption<T> option, T value) {
    mNettyServerBuilder = mNettyServerBuilder.withChildOption(option, value);
    return this;
  }

  /**
   * Sets the boss {@link EventLoopGroup}.
   *
   * @param bossGroup the boss event loop group
   * @return an updated instance of this {@link GrpcServerBuilder}
   */
  public GrpcServerBuilder bossEventLoopGroup(EventLoopGroup bossGroup) {
    mNettyServerBuilder = mNettyServerBuilder.bossEventLoopGroup(bossGroup);
    return this;
  }

  /**
   * Sets the worker {@link EventLoopGroup}.
   *
   * @param workerGroup the worker event loop group
   * @return an updated instance of this {@link GrpcServerBuilder}
   */
  public GrpcServerBuilder workerEventLoopGroup(EventLoopGroup workerGroup) {
    mNettyServerBuilder = mNettyServerBuilder.workerEventLoopGroup(workerGroup);
    return this;
  }

  /**
   * Sets the maximum size of inbound messages.
   * @param messageSize maximum size of the message
   * @return an updated instance of this {@link GrpcServerBuilder}
   */
  public GrpcServerBuilder maxInboundMessageSize(int messageSize) {
    mNettyServerBuilder = mNettyServerBuilder.maxInboundMessageSize(messageSize);
    return this;
  }

  /**
   * Add a service to this server.
   *
   * @param serviceType the type of service
   * @param serviceDefinition the service definition of new service
   * @return an updated instance of this {@link GrpcServerBuilder}
   */
  public GrpcServerBuilder addService(ServiceType serviceType, GrpcService serviceDefinition) {
    mServices.add(serviceType);
    return addService(serviceDefinition);
  }

  /**
   * Add a service to this server.
   *
   * @param serviceDefinition the service definition of new service
   * @return an updated instance of this {@link GrpcServerBuilder}
   */
  public GrpcServerBuilder addService(GrpcService serviceDefinition) {
    ServerServiceDefinition service = serviceDefinition.getServiceDefinition();
    if (SecurityUtils.isAuthenticationEnabled(mConfiguration)
        && serviceDefinition.isAuthenticated()) {
      // Intercept the service with authenticated user injector.
      service = ServerInterceptors.intercept(service,
          new AuthenticatedUserInjector(mAuthenticationServer));
    }
    mNettyServerBuilder = mNettyServerBuilder.addService(service);
    mCloser.register(serviceDefinition.getCloser());
    return this;
  }

  /**
   * Adds an interceptor for this server.
   *
   * @param interceptor server interceptor
   * @return an updates instance of this {@link GrpcServerBuilder}
   */
  public GrpcServerBuilder intercept(ServerInterceptor interceptor) {
    mNettyServerBuilder = mNettyServerBuilder.intercept(interceptor);
    return this;
  }

  /**
   * Sets TLS context.
   *
   * @param sslContext TLS context
   * @return an updated instance of this {@link GrpcServerBuilder}
   */
  public GrpcServerBuilder sslContext(SslContext sslContext) {
    mNettyServerBuilder = mNettyServerBuilder.sslContext(sslContext);
    return this;
  }

  /**
   * Build the server.
   * It attaches required services and interceptors for authentication.
   *
   * @return the built {@link GrpcServer}
   */
  public GrpcServer build() {
    addService(new GrpcService(new ServiceVersionClientServiceHandler(mServices))
        .disableAuthentication());
    return new GrpcServer(mNettyServerBuilder.build(), mAuthenticationServer, mCloser,
        mConfiguration.getMs(PropertyKey.NETWORK_CONNECTION_SERVER_SHUTDOWN_TIMEOUT));
  }
}
