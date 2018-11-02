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

import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.netty.NettyChannelBuilder;

/**
 * A simple wrapper around the {@link NettyChannelBuilder} class in grpc. Outside of this module,
 * this class should be used to replace references to {@link NettyChannelBuilder} for dependency
 * management. Note: This class is intended for internal use only.
 */
public final class GrpcChannelBuilder {

  NettyChannelBuilder mNettyChannelBuilder;

  /**
   * Create a channel builder for given address.
   *
   * @param name the host name
   * @param port the hos port
   * @return a new instance of {@link GrpcChannelBuilder}
   */
  public static GrpcChannelBuilder forAddress(String name, int port) {
    return new GrpcChannelBuilder(NettyChannelBuilder.forAddress(name, port));
  }

  private GrpcChannelBuilder(NettyChannelBuilder nettyChannelBuilder) {
    mNettyChannelBuilder = nettyChannelBuilder.nameResolverFactory(new DnsNameResolverProvider());
  }

  /**
   * Whether to use plain text.
   *
   * @param skipNegotiation whether to skip negotiation
   * @return the updated {@link GrpcChannelBuilder} instance
   */
  public GrpcChannelBuilder usePlaintext(boolean skipNegotiation) {
    mNettyChannelBuilder = mNettyChannelBuilder.usePlaintext(skipNegotiation);
    return this;
  }

  /**
   * @return the built {@link GrpcChannel}
   */
  public GrpcChannel build() {
    return new GrpcChannel(mNettyChannelBuilder.build());
  }
}
