/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.grpc;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.authentication.AuthenticationServer;
import alluxio.security.authentication.DefaultAuthenticationServer;
import alluxio.util.SecurityUtils;

import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
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

  /** Configuration object used for  */
  private AlluxioConfiguration mConfiguration;

  private GrpcServerBuilder(NettyServerBuilder nettyServerBuilder, AlluxioConfiguration conf) {
    mConfiguration = conf;
    mServices = new HashSet<>();
    mNettyServerBuilder = nettyServerBuilder;
    if (SecurityUtils.isAuthenticationEnabled(conf)) {
      LoggerFactory.getLogger(GrpcServerBuilder.class).warn("Authentication ENABLED");
      mAuthenticationServer = new DefaultAuthenticationServer(conf);
      addService(new GrpcService(mAuthenticationServer).disableAuthentication());
    }
  }

  /**
   * Create an new instance of {@link GrpcServerBuilder} with authentication support.
   *
   * @param address the address
   * @return a new instance of {@link GrpcServerBuilder}
   */
  public static GrpcServerBuilder forAddress(SocketAddress address, AlluxioConfiguration conf) {
    return new GrpcServerBuilder(NettyServerBuilder.forAddress(address), conf);
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
    if (SecurityUtils.isAuthenticationEnabled(mConfiguration) && serviceDefinition.isAuthenticated()) {
      service = ServerInterceptors.intercept(service, mAuthenticationServer.getInterceptors());
    }
    mNettyServerBuilder = mNettyServerBuilder.addService(service);
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
   * Build the server.
   * It attaches required services and interceptors for authentication.
   *
   * @return the built {@link GrpcServer}
   */
  public GrpcServer build() {
    addService(new GrpcService(new ServiceVersionClientServiceHandler(mServices))
        .disableAuthentication());
    return new GrpcServer(mNettyServerBuilder.build(),
        mConfiguration.getMs(PropertyKey.MASTER_GRPC_SERVER_SHUTDOWN_TIMEOUT));
  }
}
