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

import alluxio.security.authentication.AuthenticationServer;
import alluxio.security.authentication.DefaultAuthenticationServer;
import alluxio.util.SecurityUtils;
import io.grpc.BindableService;
import io.grpc.ServerInterceptor;
import io.grpc.netty.NettyServerBuilder;

import java.net.InetSocketAddress;
import java.util.concurrent.Executor;

import javax.annotation.Nullable;

/**
 * Provides authenticated gRPC server creation.
 */
public class GrpcServerBuilder {

  NettyServerBuilder mNettyServerBuilder;
  AuthenticationServer mAuthenticationServer;

  private GrpcServerBuilder(NettyServerBuilder nettyChannelBuilder) {
    mNettyServerBuilder = nettyChannelBuilder;
    mAuthenticationServer = new DefaultAuthenticationServer();
  }

  /**
   * Create an new instance of {@link GrpcServerBuilder} with authentication support.
   *
   * @param address the address
   * @return a new instance of {@link GrpcServerBuilder}
   */
  public static GrpcServerBuilder forAddress(InetSocketAddress address) {
    return new GrpcServerBuilder(NettyServerBuilder.forAddress(address));
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
   * Add a service to this server.
   *
   * @param service the service to add
   * @return an updated instance of this {@link GrpcServerBuilder}
   */
  public GrpcServerBuilder addService(BindableService service) {
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
   * Build.
   *
   * @return the built {@link GrpcServer}
   */
  public GrpcServer build() {
    // Add authentication service and interceptors to server that is being built.
    if (SecurityUtils.isAuthenticationEnabled()) {
      mNettyServerBuilder.addService(mAuthenticationServer);
      for (ServerInterceptor interceptor : mAuthenticationServer.getInterceptors()) {
        mNettyServerBuilder.intercept(interceptor);
      }
    }
    return new GrpcServer(mNettyServerBuilder.build());
  }

}
