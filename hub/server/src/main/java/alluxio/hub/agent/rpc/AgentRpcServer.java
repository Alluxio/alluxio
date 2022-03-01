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

package alluxio.hub.agent.rpc;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.hub.agent.process.AgentProcessContext;
import alluxio.hub.agent.rpc.service.AgentManagerService;
import alluxio.security.authentication.AuthenticationServer;
import alluxio.security.authentication.DefaultAuthenticationServer;
import alluxio.security.user.ServerUserState;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * A wrapper around a {@link GrpcServer} that utilizes the Hub Agent configuration.
 *
 * It will generate and utilize a self-signed certificate to encrypt the traffic between itself
 * and clients. This requires all clients connecting to this server use an insecure client that
 * trusts self-signed certificates. Its slightly better than plaintext, but still insecure as it
 * can easily be MITMed.
 *
 * In the future, we may allow configuring certificates properly.
 *
 * The server starts serving after object creation. To stop the server call the {@link #close()}
 * method.
 */
public class AgentRpcServer implements AutoCloseable {
  private final GrpcServer mServer;

  /**
   * Creates and starts a new {@link GrpcServer}.
   * @param conf alluxio configuration
   * @param ctx the context used to instantiate the rpc services
   * @throws IOException if the server fails to start
   */
  public AgentRpcServer(AlluxioConfiguration conf, AgentProcessContext ctx) throws IOException {
    GrpcServerAddress grpcAddr = GrpcServerAddress.create(getConfiguredAddress(conf));
    AuthenticationServer authServer = new DefaultAuthenticationServer(grpcAddr.getHostName(), conf);
    mServer = GrpcServerBuilder
        .forAddress(grpcAddr, authServer, conf, ServerUserState.global())
        .addService(new GrpcService(new AgentManagerService(ctx).bindService()))
        .build();
    mServer.start();
  }

  /**
   * @param conf alluxio configuration
   * @return the address that this server will attempt to use based on the configuration
   */
  public static InetSocketAddress getConfiguredAddress(AlluxioConfiguration conf) {
    return new InetSocketAddress(
        conf.get(PropertyKey.HUB_AGENT_RPC_BIND_HOST),
        conf.getInt(PropertyKey.HUB_AGENT_RPC_PORT));
  }

  /**
   * @return the port of the running server
   */
  public int getPort() {
    return mServer.getBindPort();
  }

  /**
   * @return whether the server is serving requests
   */
  public boolean isServing() {
    return mServer.isServing();
  }

  /**
   * Blocks until the server is terminated.
   */
  public void awaitTermination() {
    mServer.awaitTermination();
  }

  @Override
  public void close() {
    mServer.shutdown();
  }
}
