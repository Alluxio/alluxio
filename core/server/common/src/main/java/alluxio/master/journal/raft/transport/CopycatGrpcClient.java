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

  /**
   * Creates copycat transport client that can be used to connect to remote copycat servers.
   *
   * @param conf Alluxio configuration
   * @param userState authentication user
   */
  public CopycatGrpcClient(AlluxioConfiguration conf, UserState userState) {
    mConf = conf;
    mUserState = userState;
    mConnections = Collections.synchronizedList(new LinkedList<>());
  }

  @Override
  public CompletableFuture<Connection> connect(Address address) {
    return ThreadContext.currentContextOrThrow().execute(() -> {
      try {
        // Create a new gRPC channel for requested connection.
        GrpcChannel channel = GrpcChannelBuilder
                .newBuilder(new GrpcServerAddress(address.host(), address.socketAddress()), mConf)
                .setSubject(mUserState.getSubject()).build();

        // Create stub for receiving stream from server.
        CopycatMessageServerGrpc.CopycatMessageServerStub messageClientStub =
            CopycatMessageServerGrpc.newStub(channel);

        // Create client connection that is bound to remote server stream.
        CopycatGrpcConnection clientConnection =
            new CopycatGrpcClientConnection(ThreadContext.currentContextOrThrow(), channel,
                mConf.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT));
        clientConnection.setTargetObserver(messageClientStub.connect(clientConnection));

        LOG.debug("Created copycat connection for target: {}", address);
        mConnections.add(clientConnection);
        return clientConnection;
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    LOG.debug("Closing copycat transport client with {} connections.", mConnections.size());
    // Close created connections.
    List<CompletableFuture<Void>> connectionCloseFutures = new ArrayList<>(mConnections.size());
    for (Connection connectionPair : mConnections) {
      connectionCloseFutures.add(connectionPair.close());
    }
    mConnections.clear();
    return CompletableFuture.allOf(connectionCloseFutures.toArray(new CompletableFuture[0]));
  }
}
