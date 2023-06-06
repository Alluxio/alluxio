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

import alluxio.conf.AlluxioConfiguration;
import alluxio.grpc.CacheRequest;
import alluxio.grpc.ClearMetricsRequest;
import alluxio.grpc.ClearMetricsResponse;
import alluxio.grpc.CompleteFilePRequest;
import alluxio.grpc.CompleteFilePResponse;
import alluxio.grpc.CopyRequest;
import alluxio.grpc.CopyResponse;
import alluxio.grpc.CreateDirectoryPRequest;
import alluxio.grpc.CreateDirectoryPResponse;
import alluxio.grpc.CreateFilePRequest;
import alluxio.grpc.CreateFilePResponse;
import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.grpc.DeletePRequest;
import alluxio.grpc.DeletePResponse;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.GetStatusPResponse;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.ListStatusPRequest;
import alluxio.grpc.ListStatusPResponse;
import alluxio.grpc.LoadFileRequest;
import alluxio.grpc.LoadFileResponse;
import alluxio.grpc.LoadRequest;
import alluxio.grpc.LoadResponse;
import alluxio.grpc.MoveBlockRequest;
import alluxio.grpc.MoveBlockResponse;
import alluxio.grpc.MoveRequest;
import alluxio.grpc.MoveResponse;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.RemoveBlockRequest;
import alluxio.grpc.RemoveBlockResponse;
import alluxio.grpc.RenamePRequest;
import alluxio.grpc.RenamePResponse;
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteResponse;
import alluxio.security.user.UserState;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

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
     * @param alluxioConf Alluxio configuration
     * @return a new {@link BlockWorkerClient}
     */
    public static BlockWorkerClient create(UserState userState, GrpcServerAddress address,
        AlluxioConfiguration alluxioConf)
        throws IOException {
      return new DefaultBlockWorkerClient(userState, address, alluxioConf);
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
   * Reads a block with getting actual data.
   *
   * @param request the read request
   * @return the stream observer for the client request
   */
  ListenableFuture<Object> readBlockNoDataBack(ReadRequest request);

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
   * 1. Client sends an open request through the request stream. Server will respond with the name
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

  /**
   * Free this worker.
   */
  void freeWorker();

  /**
   * load blocks into alluxio.
   *
   * @param request the cache request
   * @return listenable future of LoadResponse
   * @throws StatusRuntimeException if any error occurs
   */
  ListenableFuture<LoadResponse> load(LoadRequest request);

  /**
   * load files.
   * @param request
   * @return listenable future of LoadFileResponse
   */
  ListenableFuture<LoadFileResponse> loadFile(LoadFileRequest request);

  /**
   * get file status.
   *
   * @param request
   * @return status
   */
  GetStatusPResponse getStatus(GetStatusPRequest request);

  /**
   * List status from Worker.
   * @param request
   * @return List of Status
   */
  Iterator<ListStatusPResponse> listStatus(ListStatusPRequest request);

  /**
   * copy files from src to dst.
   *
   * @param request the copy request
   * @return listenable future of CopyResponse
   * @throws StatusRuntimeException if any error occurs
   */
  ListenableFuture<CopyResponse> copy(CopyRequest request);

  /**
   * move files from src to dst.
   *
   * @param request the move request
   * @return listenable future of MoveResponse
   * @throws StatusRuntimeException if any error occurs
   */
  ListenableFuture<MoveResponse> move(MoveRequest request);

  /**
   * Create file request from client to worker.
   * @param request the request to create a file
   * @return a response to contain FileInfo and OpenHandle
   */
  CreateFilePResponse createFile(CreateFilePRequest request);

  /**
   * Complete a file when writing is done.
   * @param request the request to complete a file
   * @return a response of this operation
   */
  CompleteFilePResponse completeFile(CompleteFilePRequest request);

  /**
   * Delete a file from.
   * @param request the request to delete a file
   * @return a response of this operation
   */
  DeletePResponse delete(DeletePRequest request);

  /**
   * Rename the src to dst.
   * @param request the request to do rename
   * @return a response of this operation
   */
  RenamePResponse rename(RenamePRequest request);

  /**
   * Create a directory.
   * @param request the request to create a dir
   * @return a response of this operation
   */
  CreateDirectoryPResponse createDirectory(CreateDirectoryPRequest request);
}
