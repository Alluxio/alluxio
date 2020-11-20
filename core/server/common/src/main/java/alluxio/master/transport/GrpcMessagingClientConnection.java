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

import alluxio.grpc.GrpcChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * {@link GrpcMessagingConnection} implementation for clients.
 */
public class GrpcMessagingClientConnection extends GrpcMessagingConnection {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcMessagingClientConnection.class);

  /** Underlying gRPC channel. */
  private final GrpcChannel mChannel;

  /**
   * Creates a messaging connection for client.
   *
   * Note: {@link #setTargetObserver} should be called explicitly before using the connection.
   *
   * @param context catalyst thread context
   * @param executor transport executor
   * @param channel underlying gRPC channel
   * @param requestTimeoutMs timeout in milliseconds for requests
   */
  public GrpcMessagingClientConnection(GrpcMessagingContext context, ExecutorService executor,
      GrpcChannel channel, long requestTimeoutMs) {
    super(ConnectionOwner.CLIENT, channel.toStringShort(), context, executor, requestTimeoutMs);
    mChannel = channel;
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> resultFuture = new CompletableFuture<>();
    super.close().whenComplete((result, error) -> {
      try {
        mChannel.shutdown();
      } catch (Exception e) {
        LOG.warn("Failed to close channel: {}. Error: {}", mChannel.toStringShort(), e);
      } finally {
        resultFuture.complete(null);
      }
    });
    return resultFuture;
  }
}
