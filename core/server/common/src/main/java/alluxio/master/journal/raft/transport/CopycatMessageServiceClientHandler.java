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

import alluxio.grpc.CopycatMessage;
import alluxio.grpc.CopycatMessageServerGrpc;

import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.transport.Connection;
import io.grpc.stub.StreamObserver;

import java.util.function.Consumer;

/**
 * gRPC service handler for message exchange between copycat clients and servers that are created by
 * {@link CopycatGrpcTransport}.
 */
public class CopycatMessageServiceClientHandler
    extends CopycatMessageServerGrpc.CopycatMessageServerImplBase {

  /** Copycat provided listener for storing incoming connections. */
  private Consumer<Connection> mListener;
  /** {@link CopycatGrpcServer}'s copycat thread context. */
  private ThreadContext mContext;
  /** Request timeout value for new connections. */
  private long mRequestTimeoutMs;

  /**
   * @param listener listener for incoming connections
   * @param context copycat thread context
   * @param requestTimeoutMs request timeout value for new connections
   */
  public CopycatMessageServiceClientHandler(Consumer<Connection> listener, ThreadContext context,
      long requestTimeoutMs) {
    mListener = listener;
    mContext = context;
    mRequestTimeoutMs = requestTimeoutMs;
  }

  /**
   * RPC for establishing bi-di stream between client/server.
   *
   * @param responseObserver client's stream observer
   * @return server's stream observer
   */
  public StreamObserver<CopycatMessage> connect(StreamObserver<CopycatMessage> responseObserver) {
    // Create server connection that is bound to given client stream.
    CopycatGrpcConnection clientConnection = new CopycatGrpcConnection(
        CopycatGrpcConnection.ConnectionOwner.SERVER, mContext, mRequestTimeoutMs);
    clientConnection.setTargetObserver(responseObserver);

    // Update copycat with new connection.
    mContext.executor().execute(() -> {
      mListener.accept(clientConnection);
    });

    return clientConnection;
  }
}
