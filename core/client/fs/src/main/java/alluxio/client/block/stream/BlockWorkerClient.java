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

package alluxio.client.block.stream;

import alluxio.grpc.CacheRequest;
import alluxio.grpc.ClearMetricsRequest;
import alluxio.grpc.ClearMetricsResponse;
import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.MoveBlockRequest;
import alluxio.grpc.MoveBlockResponse;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.conf.AlluxioConfiguration;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.RemoveBlockRequest;
import alluxio.grpc.RemoveBlockResponse;
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteResponse;
import alluxio.security.user.UserState;

import io.grpc.stub.StreamObserver;
import io.grpc.StatusRuntimeException;

import java.io.Closeable;
import java.io.IOException;

/**
 * gRPC client for worker communication.
 */
public interface BlockWorkerClient extends Closeable {

  /**
   * Factory for block worker client.
   */
  class Factory {
    /**
     * Creates a new block worker client.
     *
     * @param userState the user subject
     * @param address the address of the worker
     * @return a new {@link BlockWorkerClient}
     */
    public static BlockWorkerClient create(UserState userState, GrpcServerAddress address,
        AlluxioConfiguration alluxioConf)
        throws IOException {
      try {
        return new DefaultBlockWorkerClient(userState, address, alluxioConf);
      } catch (Exception e) {
        throw new IOException(
            String.format("Failed to connect to remote block worker: %s", address), e);
      }
    }
  }

  /**
   * @return whether the client is shutdown
   */
  boolean isShutdown();

  /**
   * @return whether the client is healthy
   */
  boolean isHealthy();

  /**
   * Writes a block to the worker asynchronously. The caller should pass in a response observer
   * for receiving server responses and handling errors.
   *
   * @param responseObserver the stream observer for the server response
   * @return the stream observer for the client request
   */
  StreamObserver<WriteRequest> writeBlock(StreamObserver<WriteResponse> responseObserver);

  /**
   * Reads a block from the worker. When client is done with the file, it should close the stream
   * using the gRPC context.
   *
   * @param responseObserver the stream observer for the server response
   * @return the stream observer for the client request
   */
  StreamObserver<ReadRequest> readBlock(StreamObserver<ReadResponse> responseObserver);

  /**
   * Creates a local block on the worker. This is a two stage operations:
   * 1. Client sends a create request through the request stream. Server will respond with the name
   *    of the file to write to.
   * 2. When client is done with the file, it should signal complete or cancel on the request stream
   *    based on the intent. The server will signal complete on the response stream once the
   *    operation is done.
   *
   * @param responseObserver the stream observer for the server response
   * @return the stream observer for the client request
   */
  StreamObserver<CreateLocalBlockRequest> createLocalBlock(
      StreamObserver<CreateLocalBlockResponse> responseObserver);

  /**
   * Opens a local block. This is a two stage operations:
   * 1. Client sends a open request through the request stream. Server will respond with the name
   *    of the file to read from.
   * 2. When client is done with the file, it should close the stream.
   *
   * @param responseObserver the stream observer for the server response
   * @return the stream observer for the client request
   */
  StreamObserver<OpenLocalBlockRequest> openLocalBlock(
      StreamObserver<OpenLocalBlockResponse> responseObserver);

  /**
   * Removes a block from worker.
   * @param request the remove block request
   * @return the response from server
   * @throws StatusRuntimeException if any error occurs
   */
  RemoveBlockResponse removeBlock(RemoveBlockRequest request);

  /**
   * Move a block from worker.
   * @param request the remove block request
   * @return the response from server
   * @throws StatusRuntimeException if any error occurs
   */
  MoveBlockResponse moveBlock(MoveBlockRequest request);

  /**
   * Clear the worker metrics.
   *
   * @param request the request to clear metrics
   * @return the response from server
   */
  ClearMetricsResponse clearMetrics(ClearMetricsRequest request);

  /**
   * Caches a block.
   *
   * @param request the cache request
   * @throws StatusRuntimeException if any error occurs
   */
  void cache(CacheRequest request);
}
