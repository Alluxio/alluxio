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

package alluxio.worker.block;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.security.user.ServerUserState;
import alluxio.security.user.UserState;

import io.grpc.BindableService;

import java.net.InetSocketAddress;

public class BlockMasterWorkerServiceTestUtils {

  /**
   * Create a {@link GrpcServer} with the specified service and socket address with default
   * configurations.
   *
   * @param serviceHandler the handler service for the server
   * @param serverAddress the socket address the server listens at
   * @return a GrpcServer instance
   */
  public static <T extends BindableService> GrpcServer createServerWithService(
      ServiceType serviceType,
      T serviceHandler,
      InetSocketAddress serverAddress
  ) {
    return createServerWithService(serviceType, serviceHandler,
        serverAddress, Configuration.global(), ServerUserState.global());
  }

  /**
   * Create a {@link GrpcServer} with the specified service and socket address, using custom
   * configurations.
   *
   * @param serviceHandler the handler service for the server
   * @param serverAddress the socket address the server listens at
   * @param conf AlluxioConfiguration for the server
   * @param userState UserState for the server
   * @return a GrpcServer instance
   */
  public static <T extends BindableService> GrpcServer createServerWithService(
      ServiceType serviceType,
      T serviceHandler,
      InetSocketAddress serverAddress,
      AlluxioConfiguration conf,
      UserState userState
  ) {
    return GrpcServerBuilder.forAddress(
        GrpcServerAddress.create(serverAddress),
        conf,
        userState)
        .addService(serviceType, new GrpcService(serviceHandler))
        .build();
  }

  /**
   * Create a {@link GrpcChannel}.
   * Connect to the given serverAddress using default configuration.
   *
   * @param serverAddress server address to connect to
   * @return created GrpcChannel
   * @throws AlluxioStatusException when failed to establish a connection with the address
   */
  public static GrpcChannel createChannel(
      InetSocketAddress serverAddress) throws AlluxioStatusException {
    return createChannel(serverAddress, Configuration.global());
  }

  /**
   * Create a {@link GrpcChannel}.
   * Connect to the given serverAddress with specified configuration.
   *
   * @param socketAddress server address to connect to
   * @param conf Alluxio Configuration to use for the channel
   * @return created GrpcChannel
   * @throws AlluxioStatusException when failed to establish a connection with the address
   */
  public static GrpcChannel createChannel(
      InetSocketAddress socketAddress, AlluxioConfiguration conf) throws AlluxioStatusException {

    return GrpcChannelBuilder.newBuilder(
        GrpcServerAddress.create(socketAddress), conf)
        .build();
  }
}
