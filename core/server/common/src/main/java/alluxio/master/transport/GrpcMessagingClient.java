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
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.MessagingServiceGrpc;
import alluxio.security.user.UserState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Client implementation based on Alluxio gRPC messaging.
 *
 * Listen should be called once for each distinct address.
 * Pending futures should all be closed prior to calling {@link #close()}.
 */
public class GrpcMessagingClient {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcMessagingClient.class);

  /** Alluxio configuration. */
  private final AlluxioConfiguration mConf;
  /** Authentication user. */
  private final UserState mUserState;

  /** Executor for building client connections. */
  private final ExecutorService mExecutor;

  /** Client type of transport. */
  private final String mClientType;

  /**
   * Creates messaging client that can be used to connect to remote messaging servers.
   *
   * @param conf Alluxio configuration
   * @param userState authentication user
   * @param executor messaging executor
   * @param clientType transport client type
   */
  public GrpcMessagingClient(AlluxioConfiguration conf, UserState userState,
      ExecutorService executor, String clientType) {
    mConf = conf;
    mUserState = userState;
    mExecutor = executor;
    mClientType = clientType;
  }

  /**
   * Creates a client and connects to the given address.
   *
   * @param address the server address
   * @return future of connection result
   */
  public CompletableFuture<GrpcMessagingConnection> connect(InetSocketAddress address) {
    LOG.debug("Creating a messaging client connection to: {}", address);
    final GrpcMessagingContext threadContext = GrpcMessagingContext.currentContextOrThrow();
    // Future for this connection.
    final CompletableFuture<GrpcMessagingConnection> connectionFuture = new CompletableFuture<>();
    // Spawn gRPC connection building on a common pool.
    final CompletableFuture<GrpcMessagingConnection> buildFuture = CompletableFuture
        .supplyAsync(() -> {
          try {
            // Create a new gRPC channel for requested connection.
            GrpcChannel channel = GrpcChannelBuilder
                .newBuilder(GrpcServerAddress.create(address.getHostString(), address), mConf)
                .setClientType(mClientType).setSubject(mUserState.getSubject())
                .build();

            // Create stub for receiving stream from server.
            MessagingServiceGrpc.MessagingServiceStub messageClientStub =
                    MessagingServiceGrpc.newStub(channel);

            // Create client connection that is bound to remote server stream.
            GrpcMessagingConnection clientConnection =
                new GrpcMessagingClientConnection(threadContext, mExecutor, channel,
                    mConf.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_TRANSPORT_REQUEST_TIMEOUT_MS));
            clientConnection.setTargetObserver(messageClientStub.connect(clientConnection));

            LOG.debug("Created a messaging client connection: {}", clientConnection);
            return clientConnection;
          } catch (Throwable e) {
            throw new RuntimeException(e);
          }
        }, mExecutor);
    // When connection is build, complete the connection future with it on a catalyst thread context
    // for setting up the connection.
    buildFuture.whenComplete((result, error) -> {
      threadContext.execute(() -> {
        if (error == null) {
          connectionFuture.complete(result);
        } else {
          connectionFuture.completeExceptionally(error);
        }
      });
    });
    return connectionFuture;
  }

  /**
   * Closes the client.
   *
   * @return future of result
   */
  public CompletableFuture<Void> close() {
    LOG.debug("Closing messaging client; {}", this);
    // Nothing to clean up
    return CompletableFuture.completedFuture(null);
  }
}
