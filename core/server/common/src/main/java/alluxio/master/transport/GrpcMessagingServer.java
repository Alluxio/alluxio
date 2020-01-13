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

package alluxio.master.transport;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
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
 * {@link Server} implementation based on Alluxio gRPC messaging.
 *
 * Each server should listen only one address.
 * Outstanding listen futures should be completed before calling {@link #close()}.
 */
public class GrpcMessagingServer implements Server {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcMessagingServer.class);

  /** Alluxio configuration. */
  private final AlluxioConfiguration mConf;
  /** Authentication user. */
  private final UserState mUserState;

  /** Underlying gRPC server. */
  private alluxio.grpc.GrpcServer mGrpcServer;
  /** Listen future. */
  private CompletableFuture<Void> mListenFuture;

  /** List of all connections created by this server. */
  private final List<Connection> mConnections;

  /** Executor for building server listener. */
  private final ExecutorService mExecutor;

  /** Proxy configuration for server connections. */
  private final GrpcMessagingProxy mProxy;

  /**
   * Creates a transport server that can be used to accept connections from remote clients.
   *
   * @param conf Alluxio configuration
   * @param userState authentication user
   * @param executor transport executor
   * @param proxy external proxy configuration
   */
  public GrpcMessagingServer(AlluxioConfiguration conf, UserState userState,
      ExecutorService executor, GrpcMessagingProxy proxy) {
    mConf = conf;
    mUserState = userState;
    mExecutor = executor;
    mProxy = proxy;
    mConnections = Collections.synchronizedList(new LinkedList<>());
  }

  @Override
  public synchronized CompletableFuture<Void> listen(Address address,
      Consumer<Connection> listener) {
    // Return existing future if building currently.
    if (mListenFuture != null && !mListenFuture.isCompletedExceptionally()) {
      return mListenFuture;
    }

    LOG.debug("Opening messaging server for: {}", address);

    final ThreadContext threadContext = ThreadContext.currentContextOrThrow();
    mListenFuture = CompletableFuture.runAsync(() -> {
      // Listener that notifies both this server instance and given listener.
      Consumer<Connection> forkListener = (connection) -> {
        addNewConnection(connection);
        listener.accept(connection);
      };

      Address bindAddress = address;
      if (mProxy.hasProxyFor(address)) {
        bindAddress = mProxy.getProxyFor(address);
        LOG.debug("Found proxy: {} for address: {}", bindAddress, address);
      }
      LOG.debug("Binding messaging server to: {}", bindAddress);

      // Create gRPC server.
      mGrpcServer = GrpcServerBuilder
          .forAddress(GrpcServerAddress.create(bindAddress.host(),
              new InetSocketAddress(bindAddress.host(), bindAddress.port())), mConf, mUserState)
          .maxInboundMessageSize((int) mConf
              .getBytes(PropertyKey.MASTER_EMBEDDED_JOURNAL_TRANSPORT_MAX_INBOUND_MESSAGE_SIZE))
          .addService(new GrpcService(ServerInterceptors.intercept(
              new GrpcMessagingServiceClientHandler(address, forkListener, threadContext,
                  mExecutor, mConf.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT)),
              new ClientIpAddressInjector())))
          .build();

      try {
        mGrpcServer.start();

        LOG.info("Successfully started messaging server at: {}", bindAddress);
      } catch (IOException e) {
        mGrpcServer = null;
        LOG.debug("Failed to create messaging server for: {}.", address, e);
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

    LOG.debug("Closing messaging server: {}", mGrpcServer);
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
            LOG.warn("Failed to close messaging gRPC server: {}", mGrpcServer);
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
