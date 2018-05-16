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

import java.util.concurrent.Executor;

import javax.annotation.Nullable;

/**
 * A simple wrapper around the {@link NettyServerBuilder} class in grpc. Outside of this module,
 * this class should be used to replace references to {@link NettyServerBuilder} for dependency
 * management. Note: This class is intended for internal use only.
 */
public class GrpcServerBuilder {

  NettyServerBuilder mNettyServerBuilder;

  public static GrpcServerBuilder forPort(int port) {
    return new GrpcServerBuilder(NettyServerBuilder.forPort(port));
  }

  private GrpcServerBuilder(NettyServerBuilder nettyChannelBuilder) {
    mNettyServerBuilder = nettyChannelBuilder;
  }

  public GrpcServerBuilder addService(BindableService service) {
    mNettyServerBuilder = mNettyServerBuilder.addService(service);
    return this;
  }

  public GrpcServer build() {
    return new GrpcServer(mNettyServerBuilder.build());
  }

  public GrpcServerBuilder executor(@Nullable Executor executor) {
    mNettyServerBuilder = mNettyServerBuilder.executor(executor);
    return this;
  }
}
