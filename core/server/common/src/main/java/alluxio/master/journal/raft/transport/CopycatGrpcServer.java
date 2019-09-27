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
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.security.authentication.ClientIpAddressInjector;
import alluxio.security.user.UserState;

import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.transport.Server;
import io.grpc.ServerInterceptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * Copycat transport {@link Server} implementation that uses Alluxio gRPC.
 *
 * Copycat guarantees each server will be used for listening only one address and won't be
 * recycled. It also guarantees that active listen future will be completed before calling close.
 */
public class CopycatGrpcServer implements Server {
  private static final Logger LOG = LoggerFactory.getLogger(CopycatGrpcServer.class);

  /** Alluxio configuration. */
  private final AlluxioConfiguration mConf;
  /** Authentication user. */
  private final UserState mUserState;

  /** Underlying gRPC server. */
  private GrpcServer mGrpcServer;
  /** Listen future. */
  private CompletableFuture<Void> mListenFuture;

  /** List of all connections created by this server. */
  private final List<Connection> mConnections;

  /** Executor for building server listener. */
  private final ExecutorService mExecutor;

  /**
   * Creates copycat transport server that can be used to accept connections from remote copycat
   * clients.
   *
   * @param conf Alluxio configuration
   * @param userState authentication user
   * @param executor transport executor
   */
  public CopycatGrpcServer(AlluxioConfiguration conf, UserState userState,
      ExecutorService executor) {
    mConf = conf;
    mUserState = userState;
    mExecutor = executor;
    mConnections = Collections.synchronizedList(new LinkedList<>());
  }

  @Override
  public synchronized CompletableFuture<Void> listen(Address address,
      Consumer<Connection> listener) {
    // Return existing future if building currently.
    if (mListenFuture != null && !mListenFuture.isCompletedExceptionally()) {
      return mListenFuture;
    }

    LOG.debug("Copycat transport server binding to: {}", address);
    final ThreadContext threadContext = ThreadContext.currentContextOrThrow();
    mListenFuture = CompletableFuture.runAsync(() -> {
      // Listener that notifies both this server instance and given listener.
      Consumer<Connection> forkListener = (connection) -> {
        addNewConnection(connection);
        listener.accept(connection);
      };

      // Create gRPC server.
      mGrpcServer = GrpcServerBuilder
          .forAddress(GrpcServerAddress.create(address.host(),
              new InetSocketAddress(address.host(), address.port())), mConf, mUserState)
          .maxInboundMessageSize((int) mConf
              .getBytes(PropertyKey.MASTER_EMBEDDED_JOURNAL_TRANSPORT_MAX_INBOUND_MESSAGE_SIZE))
          .addService(new GrpcService(ServerInterceptors.intercept(
              new CopycatMessageServiceClientHandler(address, forkListener, threadContext,
                  mExecutor, mConf.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT)),
              new ClientIpAddressInjector())))
          .build();

      try {
        mGrpcServer.start();

        LOG.info("Successfully started gRPC server for copycat transport at: {}", address);
      } catch (IOException e) {
        mGrpcServer = null;
        LOG.debug("Failed to create gRPC server for copycat transport at: {}.", address, e);
        throw new RuntimeException(e);
      }
    }, mExecutor);

    return mListenFuture;
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    if (mGrpcServer == null) {
      return CompletableFuture.completedFuture(null);
    }

    LOG.debug("Closing copycat transport server: {}", mGrpcServer);
    // Close created connections.
    List<CompletableFuture<Void>> connectionCloseFutures = new ArrayList<>(mConnections.size());
    for (Connection connection : mConnections) {
      connectionCloseFutures.add(connection.close());
    }
    mConnections.clear();

    CompletableFuture<Void> future = new CompletableFuture<>();
    CompletableFuture.allOf(connectionCloseFutures.toArray(new CompletableFuture[0]))
        .whenComplete((result, error) -> {
          // Shut down gRPC server once all connections are closed.
          try {
            mGrpcServer.shutdown();
          } catch (Exception e) {
            LOG.warn("Failed to close copycat transport server: {}", mGrpcServer);
          } finally {
            mGrpcServer = null;
          }
          // Complete the future with result from connection shut downs.
          if (error == null) {
            future.complete(result);
          } else {
            future.completeExceptionally(error);
          }
        });
    return future;
  }

  /**
   * Used to keep track of all connections created by this server instance.
   *
   * @param serverConnection new client connection
   */
  private synchronized void addNewConnection(Connection serverConnection) {
    mConnections.add(serverConnection);
  }
}
