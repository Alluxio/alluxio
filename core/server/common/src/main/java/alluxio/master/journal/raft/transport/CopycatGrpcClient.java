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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Copycat transport {@link Client} implementation that uses Alluxio gRPC.
 */
public class CopycatGrpcClient implements Client {
  private static final Logger LOG = LoggerFactory.getLogger(CopycatGrpcClient.class);

  /** Alluxio configuration. */
  private final AlluxioConfiguration mConf;
  /** Authentication user. */
  private final UserState mUserState;

  /** Created channels. */
  private final Map<Address, GrpcChannel> mChannels;

  /** Created connections. */
  private final List<Connection> mConnections;

  /** Whether this client is closed. */
  private boolean mClosed = false;

  /**
   * Creates copycat transport client that can be used to connect to remote copycat servers.
   *
   * @param conf Alluxio configuration
   * @param userState authentication user
   */
  public CopycatGrpcClient(AlluxioConfiguration conf, UserState userState) {
    mConf = conf;
    mUserState = userState;

    mChannels = new HashMap<>();
    mConnections = new LinkedList<>();
  }

  @Override
  public synchronized CompletableFuture<Connection> connect(Address address) {
    CompletableFuture<Connection> resultFuture = new CompletableFuture<Connection>();
    try {
      // Create if there is no existing channel to given address.
      if (!mChannels.containsKey(address)) {
        LOG.debug("Creating gRPC channel for target: {}", address);
        mChannels.put(address,
            GrpcChannelBuilder
                .newBuilder(new GrpcServerAddress(address.host(), address.socketAddress()), mConf)
                .setSubject(mUserState.getSubject()).build());
      }

      // Create stub for receiving stream from server.
      CopycatMessageServerGrpc.CopycatMessageServerStub messageClientStub =
          CopycatMessageServerGrpc.newStub(mChannels.get(address));

      // Create client connection that is bound to remote server stream.
      CopycatGrpcConnection clientConnection = new CopycatGrpcConnection(
          CopycatGrpcConnection.ConnectionOwner.CLIENT, ThreadContext.currentContextOrThrow(),
          mConf.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT));
      clientConnection.setTargetObserver(messageClientStub.connect(clientConnection));

      LOG.debug("Created copycat connection for target: {}", address);
      // Complete the future.
      resultFuture.complete(clientConnection);
      mConnections.add(clientConnection);
    } catch (Throwable e) {
      // Fail the future.
      resultFuture.completeExceptionally(e);
    }

    return resultFuture;
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    if (!mClosed) {
      LOG.debug("Closing copycat transport client with {} gRPC channels.", mChannels.size());

      // Close created connections.
      List<CompletableFuture<Void>> connectionCloseFutures = new ArrayList<>(mConnections.size());
      for (Connection connection : mConnections) {
        connectionCloseFutures.add(connection.close());
      }
      mConnections.clear();
      try {
        CompletableFuture.allOf(connectionCloseFutures.toArray(new CompletableFuture[0])).get();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(
            "Interrupted while waiting for closing existing client connections.");
      } catch (ExecutionException ee) {
        LOG.warn("Failed to close copycat transport client connections", ee.getCause());
      }

      // Shut down underlying gRPC channels.
      for (GrpcChannel channel : mChannels.values()) {
        try {
          channel.shutdown();
        } catch (Exception e) {
          LOG.debug("Failed to close underlying gRPC channel: {}", channel, e);
        }
      }
      mChannels.clear();
    }
    return CompletableFuture.completedFuture(null);
  }
}
