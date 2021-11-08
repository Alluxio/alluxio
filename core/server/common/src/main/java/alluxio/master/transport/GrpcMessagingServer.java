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

import io.grpc.ServerInterceptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * Server implementation based on Alluxio gRPC messaging.
 *
 * Each server should listen only one address.
 * Outstanding listen futures should be completed before calling {@link #close()}.
 */
public class GrpcMessagingServer {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcMessagingServer.class);

  /** Alluxio configuration. */
  private final AlluxioConfiguration mConf;
  /** Authentication user. */
  private final UserState mUserState;

  /** Underlying gRPC server. */
  private alluxio.grpc.GrpcServer mGrpcServer;
  /** Listen future. */
  private CompletableFuture<Void> mListenFuture;

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
  }

  /**
   * Listens to the given address.
   *
   * @param address the address to connect to
   * @param listener listener for new connections
   * @return completable future of the connection
   */
  public synchronized CompletableFuture<Void> listen(InetSocketAddress address,
      Consumer<GrpcMessagingConnection> listener) {
    // Return existing future if building currently.
    if (mListenFuture != null && !mListenFuture.isCompletedExceptionally()) {
      return mListenFuture;
    }

    LOG.debug("Opening messaging server for: {}", address);

    final GrpcMessagingContext threadContext = GrpcMessagingContext.currentContextOrThrow();
    mListenFuture = CompletableFuture.runAsync(() -> {

      InetSocketAddress bindAddress = address;
      if (mProxy.hasProxyFor(address)) {
        bindAddress = mProxy.getProxyFor(address);
        LOG.debug("Found proxy: {} for address: {}", bindAddress, address);
      }
      LOG.debug("Binding messaging server to: {}", bindAddress);

      // Create gRPC server.
      mGrpcServer = GrpcServerBuilder
          .forAddress(GrpcServerAddress.create(bindAddress.getHostString(),
              bindAddress), mConf, mUserState)
          .maxInboundMessageSize((int) mConf.getBytes(
              PropertyKey.MASTER_EMBEDDED_JOURNAL_TRANSPORT_MAX_INBOUND_MESSAGE_SIZE))
          .addService(new GrpcService(ServerInterceptors.intercept(
              new GrpcMessagingServiceClientHandler(address, listener::accept, threadContext,
                  mExecutor, mConf.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT)),
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

  /**
   * Closes the connection to the server if exists.
   *
   * @return the close completable future
   */
  public synchronized CompletableFuture<Void> close() {
    if (mGrpcServer == null) {
      return CompletableFuture.completedFuture(null);
    }

    LOG.debug("Closing messaging server: {}", mGrpcServer);

    return CompletableFuture.runAsync(() -> {
      try {
        mGrpcServer.shutdown();
      } catch (Exception e) {
        LOG.warn("Failed to close messaging gRPC server: {}", mGrpcServer);
      } finally {
        mGrpcServer = null;
      }
    });
  }
}
