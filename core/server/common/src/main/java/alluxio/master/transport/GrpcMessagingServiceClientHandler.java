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

import alluxio.grpc.MessagingServiceGrpc;
import alluxio.grpc.TransportMessage;
import alluxio.security.authentication.ClientIpAddressInjector;

import com.google.common.base.MoreObjects;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * gRPC service handler for message exchange between messaging clients and servers
 * that are created by {@link GrpcMessagingTransport}.
 */
public class GrpcMessagingServiceClientHandler
    extends MessagingServiceGrpc.MessagingServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcMessagingServiceClientHandler.class);

  /** Messaging server provided listener for storing incoming connections. */
  private final Consumer<GrpcMessagingConnection> mListener;
  /** {@link GrpcMessagingServer}'s catalyst thread context. */
  private final GrpcMessagingContext mContext;
  /** Request timeout value for new connections. */
  private final long mRequestTimeoutMs;
  /** Executor for building server connections. */
  private final ExecutorService mExecutor;
  /** Address for the server that is serving this handler. */
  private final InetSocketAddress mServerAddress;

  /**
   * @param serverAddress server address
   * @param listener listener for incoming connections
   * @param context catalyst thread context
   * @param executor transport executor
   * @param requestTimeoutMs request timeout value for new connections
   */
  public GrpcMessagingServiceClientHandler(InetSocketAddress serverAddress,
      Consumer<GrpcMessagingConnection> listener, GrpcMessagingContext context,
      ExecutorService executor, long requestTimeoutMs) {
    mServerAddress = serverAddress;
    mListener = listener;
    mContext = context;
    mExecutor = executor;
    mRequestTimeoutMs = requestTimeoutMs;
  }

  /**
   * RPC for establishing bi-di stream between client/server.
   *
   * @param responseObserver client's stream observer
   * @return server's stream observer
   */
  public StreamObserver<TransportMessage> connect(
      StreamObserver<TransportMessage> responseObserver) {
    // Transport level identifier for this connection.
    String transportId = MoreObjects.toStringHelper(this)
        .add("ServerAddress", mServerAddress)
        .add("ClientAddress", ClientIpAddressInjector.getIpAddress())
        .toString();
    LOG.debug("Creating a messaging server connection: {}", transportId);

    // Create server connection that is bound to given client stream.
    GrpcMessagingConnection serverConnection =
        new GrpcMessagingServerConnection(transportId, mContext, mExecutor, mRequestTimeoutMs);
    serverConnection.setTargetObserver(responseObserver);
    LOG.debug("Created a messaging server connection: {}", serverConnection);

    // Update server with new connection.
    try {
      mContext.execute(() -> {
        mListener.accept(serverConnection);
      }).get();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          "Interrupted while waiting for server to register new connection.");
    } catch (ExecutionException ee) {
      throw new RuntimeException("Failed to register new connection with server", ee.getCause());
    }

    return serverConnection;
  }
}
