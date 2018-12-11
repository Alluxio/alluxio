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

package alluxio.util.grpc;

import io.grpc.BindableService;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.ServerChannel;

import java.net.SocketAddress;
import java.util.concurrent.Executor;

import javax.annotation.Nullable;

/**
 * A simple wrapper around the {@link NettyServerBuilder} class in grpc. Outside of this module,
 * this class should be used to replace references to {@link NettyServerBuilder} for dependency
 * management. Note: This class is intended for internal use only.
 */
public class GrpcServerBuilder {

  NettyServerBuilder mNettyServerBuilder;

  /**
   * Create an new instance of {@link GrpcServerBuilder}.
   *
   * @param port the host port
   * @return a new instance of {@link GrpcServerBuilder}
   */
  public static GrpcServerBuilder forPort(int port) {
    return new GrpcServerBuilder(NettyServerBuilder.forPort(port));
  }

  /**
   * Create an new instance of {@link GrpcServerBuilder}.
   *
   * @param address the host address
   * @return a new instance of {@link GrpcServerBuilder}
   */
  public static GrpcServerBuilder forAddress(SocketAddress address) {
    return new GrpcServerBuilder(NettyServerBuilder.forAddress(address));
  }

  private GrpcServerBuilder(NettyServerBuilder nettyChannelBuilder) {
    mNettyServerBuilder = nettyChannelBuilder;
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
   * Build.
   *
   * @return the built {@link GrpcServer}
   */
  public GrpcServer build() {
    return new GrpcServer(mNettyServerBuilder.build());
  }

  /**
   * Sets flow control window.
   * @param flowControlWindow the HTTP2 flow control window
   * @return an updated instance of this {@link GrpcServerBuilder}
   */
  public GrpcServerBuilder flowControlWindow(int flowControlWindow) {
    mNettyServerBuilder = mNettyServerBuilder.flowControlWindow(flowControlWindow);
    return this;
  }

  /**
   * Sets the netty channel type.
   * @param channelType the netty channel type for the server
   * @return an updated instance of this {@link GrpcServerBuilder}
   */
  public GrpcServerBuilder channelType(Class<? extends ServerChannel> channelType) {
    mNettyServerBuilder = mNettyServerBuilder.channelType(channelType);
    return this;
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
}
