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
import alluxio.grpc.CopycatMessageServerGrpc;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcServerAddress;
import alluxio.security.user.UserState;

import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Client;
import io.atomix.catalyst.transport.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Copycat transport {@link Client} implementation that uses Alluxio gRPC.
 *
 * Copycat guarantees that listen will be called once for each distinct address.
 * It also guarantees pending futures will all be closed prior to calling close.
 */
public class CopycatGrpcClient implements Client {
  private static final Logger LOG = LoggerFactory.getLogger(CopycatGrpcClient.class);

  /** Alluxio configuration. */
  private final AlluxioConfiguration mConf;
  /** Authentication user. */
  private final UserState mUserState;

  /** Created connections. */
  private final List<Connection> mConnections;

  /** Executor for building client connections. */
  private final ExecutorService mExecutor;

  /**
   * Creates copycat transport client that can be used to connect to remote copycat servers.
   *
   * @param conf Alluxio configuration
   * @param userState authentication user
   * @param executor transport executor
   */
  public CopycatGrpcClient(AlluxioConfiguration conf, UserState userState,
      ExecutorService executor) {
    mConf = conf;
    mUserState = userState;
    mExecutor = executor;
    mConnections = Collections.synchronizedList(new LinkedList<>());
  }

  @Override
  public CompletableFuture<Connection> connect(Address address) {
    LOG.debug("Creating a copycat client connection to: {}", address);
    final ThreadContext threadContext = ThreadContext.currentContextOrThrow();
    // Future for this connection.
    final CompletableFuture<Connection> connectionFuture = new CompletableFuture<>();
    // Spawn gRPC connection building on a common pool.
    final CompletableFuture<Connection> buildFuture = CompletableFuture.supplyAsync(() -> {
      try {
        // Create a new gRPC channel for requested connection.
        GrpcChannel channel = GrpcChannelBuilder
            .newBuilder(GrpcServerAddress.create(address.host(), address.socketAddress()), mConf)
            .setClientType("CopycatClient").setSubject(mUserState.getSubject())
            .setMaxInboundMessageSize((int) mConf
                .getBytes(PropertyKey.MASTER_EMBEDDED_JOURNAL_TRANSPORT_MAX_INBOUND_MESSAGE_SIZE))
            .build();

        // Create stub for receiving stream from server.
        CopycatMessageServerGrpc.CopycatMessageServerStub messageClientStub =
            CopycatMessageServerGrpc.newStub(channel);

        // Create client connection that is bound to remote server stream.
        CopycatGrpcConnection clientConnection =
            new CopycatGrpcClientConnection(threadContext, mExecutor, channel,
                mConf.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_TRANSPORT_REQUEST_TIMEOUT_MS));
        clientConnection.setTargetObserver(messageClientStub.connect(clientConnection));

        LOG.debug("Created a copycat client connection: {}", clientConnection);
        mConnections.add(clientConnection);
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

  @Override
  public CompletableFuture<Void> close() {
    LOG.debug("Closing copycat transport client with {} connections.", mConnections.size());
    // Close created connections.
    List<CompletableFuture<Void>> connectionCloseFutures = new ArrayList<>(mConnections.size());
    for (Connection connection : mConnections) {
      connectionCloseFutures.add(connection.close());
    }
    mConnections.clear();
    return CompletableFuture.allOf(connectionCloseFutures.toArray(new CompletableFuture[0]));
  }
}
