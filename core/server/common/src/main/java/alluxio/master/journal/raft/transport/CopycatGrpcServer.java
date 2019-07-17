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

package alluxio.master.journal.raft.transport;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.security.user.UserState;

import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.transport.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Copycat transport {@link Server} implementation that uses Alluxio gRPC.
 */
public class CopycatGrpcServer implements Server {
  private static final Logger LOG = LoggerFactory.getLogger(CopycatGrpcServer.class);

  /** Alluxio configuration. */
  private AlluxioConfiguration mConf;
  /** Authentication user. */
  private UserState mUserState;

  /** Underlying gRPC server. */
  private GrpcServer mGrpcServer;
  /** Bind address for underlying server. */
  private Address mActiveAddress;

  /**
   * Creates copycat transport server that can be used to accept connections from remote copycat
   * clients.
   *
   * @param conf Alluxio configuration
   * @param userState authentication user
   */
  public CopycatGrpcServer(AlluxioConfiguration conf, UserState userState) {
    mConf = conf;
    mUserState = userState;
  }

  @Override
  public synchronized CompletableFuture<Void> listen(Address address,
      Consumer<Connection> listener) {
    LOG.debug("Copycat transport server binding to: {}", address);
    CompletableFuture<Void> resultFuture = new CompletableFuture<Void>();

    // Create gRPC server.
    mGrpcServer =
        GrpcServerBuilder.forAddress(address.host(), address.socketAddress(), mConf, mUserState)
            .addService(new GrpcService(new CopycatMessageServiceClientHandler(listener,
                ThreadContext.currentContextOrThrow(),
                mConf.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT))))
            .build();

    try {
      mGrpcServer.start();
      mActiveAddress = address;
      resultFuture.complete(null);
      LOG.info("Successfully started gRPC server for copycat transport at: {}", address);
    } catch (IOException e) {
      mGrpcServer = null;
      LOG.debug("Failed to create gRPC server for copycat transport at: {}. Error: {}", address, e);
      resultFuture.completeExceptionally(e);
    }

    return resultFuture;
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    LOG.debug("Closing copycat transport server at: {}", mActiveAddress);
    mGrpcServer.shutdown();
    return CompletableFuture.completedFuture(null);
  }
}
