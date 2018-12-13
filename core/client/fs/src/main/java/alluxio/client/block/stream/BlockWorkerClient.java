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

import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteResponse;

import io.grpc.stub.StreamObserver;

import java.io.Closeable;
import java.net.SocketAddress;
import java.util.Iterator;

import javax.security.auth.Subject;

/**
 * gRPC client for worker communication.
 */
public interface BlockWorkerClient extends Closeable {
  /**
   * Builder for the block worker client.
   */
  interface Builder {
    /**
     * @return a new {@link BlockWorkerClient}
     */
    BlockWorkerClient build();
  }

  /**
   * Gets a builder for given user subject and address.
   *
   * @param subject the user subject
   * @param address the address of the worker
   * @return the builder for the client
   */
  static Builder getBuilder(Subject subject, SocketAddress address) {
    return DefaultBlockWorkerClient.getBuilder(subject, address);
  }

  /**
   * @return whether the client is shutdown
   */
  boolean isShutdown();

  /**
   * Writes a block to the worker.
   *
   * @param responseObserver the stream observer for the server response
   * @return the stream observer for the client request
   */
  StreamObserver<WriteRequest> writeBlock(StreamObserver<WriteResponse> responseObserver);

  /**
   * Reads a block from the worker. A StatusRuntimeException will be thrown if any error occurs.
   *
   * @param request the read request
   * @return the streamed response from server
   */
  Iterator<ReadResponse> readBlock(final ReadRequest request);
}
