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

package alluxio.worker.grpc;

import alluxio.RpcUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.client.file.FileSystemContext;
import alluxio.grpc.AsyncCacheRequest;
import alluxio.grpc.AsyncCacheResponse;
import alluxio.grpc.BlockWorkerGrpc;
import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.ReadResponseMarshaller;
import alluxio.grpc.RemoveBlockRequest;
import alluxio.grpc.RemoveBlockResponse;
import alluxio.grpc.WriteRequestMarshaller;
import alluxio.grpc.WriteResponse;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.util.IdUtils;
import alluxio.util.SecurityUtils;
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.AsyncCacheRequestManager;
import alluxio.worker.block.BlockWorker;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * Server side implementation of the gRPC BlockWorker interface.
 */
@SuppressFBWarnings("BC_UNCONFIRMED_CAST")
public class BlockWorkerImpl extends BlockWorkerGrpc.BlockWorkerImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(BlockWorkerImpl.class);

  private static final boolean ZERO_COPY_ENABLED =
      ServerConfiguration.getBoolean(PropertyKey.WORKER_NETWORK_ZEROCOPY_ENABLED);
  private WorkerProcess mWorkerProcess;
  private final AsyncCacheRequestManager mRequestManager;
  private ReadResponseMarshaller mReadResponseMarshaller = new ReadResponseMarshaller();
  private WriteRequestMarshaller mWriteRequestMarshaller = new WriteRequestMarshaller();

  /**
   * Creates a new implementation of gRPC BlockWorker interface.
   *
   * @param workerProcess the worker process
   * @param fsContext context used to read blocks
   */
  public BlockWorkerImpl(WorkerProcess workerProcess, FileSystemContext fsContext) {
    mWorkerProcess = workerProcess;
    mRequestManager = new AsyncCacheRequestManager(
        GrpcExecutors.ASYNC_CACHE_MANAGER_EXECUTOR, mWorkerProcess.getWorker(BlockWorker.class),
        fsContext);
  }

  /**
   * @return a map of gRPC methods with overridden descriptors
   */
  public Map<MethodDescriptor, MethodDescriptor> getOverriddenMethodDescriptors() {
    if (ZERO_COPY_ENABLED) {
      return ImmutableMap.of(
          BlockWorkerGrpc.getReadBlockMethod(),
          BlockWorkerGrpc.getReadBlockMethod().toBuilder()
              .setResponseMarshaller(mReadResponseMarshaller).build(),
          BlockWorkerGrpc.getWriteBlockMethod(),
          BlockWorkerGrpc.getWriteBlockMethod().toBuilder()
              .setRequestMarshaller(mWriteRequestMarshaller).build());
    }
    return Collections.emptyMap();
  }

  @Override
  public StreamObserver<ReadRequest> readBlock(StreamObserver<ReadResponse> responseObserver) {
    CallStreamObserver<ReadResponse> callStreamObserver =
        (CallStreamObserver<ReadResponse>) responseObserver;
    if (ZERO_COPY_ENABLED) {
      callStreamObserver =
          new DataMessageServerStreamObserver<>(callStreamObserver, mReadResponseMarshaller);
    }
    BlockReadHandler readHandler = new BlockReadHandler(GrpcExecutors.BLOCK_READER_EXECUTOR,
        mWorkerProcess.getWorker(BlockWorker.class), callStreamObserver,
        getAuthenticatedUserInfo());
    callStreamObserver.setOnReadyHandler(readHandler::onReady);
    return readHandler;
  }

  @Override
  public StreamObserver<alluxio.grpc.WriteRequest> writeBlock(
      StreamObserver<WriteResponse> responseObserver) {
    ServerCallStreamObserver<WriteResponse> serverResponseObserver =
        (ServerCallStreamObserver<WriteResponse>) responseObserver;
    if (ZERO_COPY_ENABLED) {
      responseObserver =
          new DataMessageServerRequestObserver<>(responseObserver, mWriteRequestMarshaller, null);
    }
    DelegationWriteHandler handler =
        new DelegationWriteHandler(mWorkerProcess, responseObserver, getAuthenticatedUserInfo());
    serverResponseObserver.setOnCancelHandler(handler::onCancel);
    return handler;
  }

  @Override
  public StreamObserver<OpenLocalBlockRequest> openLocalBlock(
      StreamObserver<OpenLocalBlockResponse> responseObserver) {
    ShortCircuitBlockReadHandler handler = new ShortCircuitBlockReadHandler(
        mWorkerProcess.getWorker(BlockWorker.class), responseObserver, getAuthenticatedUserInfo());
    return handler;
  }

  @Override
  public StreamObserver<CreateLocalBlockRequest> createLocalBlock(
      StreamObserver<CreateLocalBlockResponse> responseObserver) {
    ShortCircuitBlockWriteHandler handler = new ShortCircuitBlockWriteHandler(
        mWorkerProcess.getWorker(BlockWorker.class), responseObserver, getAuthenticatedUserInfo());
    ServerCallStreamObserver<CreateLocalBlockResponse> serverCallStreamObserver =
        (ServerCallStreamObserver<CreateLocalBlockResponse>) responseObserver;
    serverCallStreamObserver.setOnCancelHandler(handler::onCancel);
    return handler;
  }

  @Override
  public void asyncCache(AsyncCacheRequest request,
      StreamObserver<AsyncCacheResponse> responseObserver) {
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<AsyncCacheResponse>) () -> {
      mRequestManager.submitRequest(request);
      return AsyncCacheResponse.getDefaultInstance();
    }, "asyncCache", "request=%s", responseObserver, request);
  }

  @Override
  public void removeBlock(RemoveBlockRequest request,
      StreamObserver<RemoveBlockResponse> responseObserver) {
    long sessionId = IdUtils.createSessionId();
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<RemoveBlockResponse>) () -> {
      mWorkerProcess.getWorker(BlockWorker.class).removeBlock(sessionId, request.getBlockId());
      return RemoveBlockResponse.getDefaultInstance();
    }, "removeBlock", "request=%s", responseObserver, request);
  }

  /**
   * @return {@link AuthenticatedUserInfo} that defines the user that has been authorized
   */
  private AuthenticatedUserInfo getAuthenticatedUserInfo() {
    try {
      if (SecurityUtils.isAuthenticationEnabled(ServerConfiguration.global())) {
        return new AuthenticatedUserInfo(
            AuthenticatedClientUser.getClientUser(ServerConfiguration.global()),
            AuthenticatedClientUser.getConnectionUser(ServerConfiguration.global()),
            AuthenticatedClientUser.getAuthMethod(ServerConfiguration.global()));
      } else {
        return new AuthenticatedUserInfo();
      }
    } catch (Exception e) {
      throw Status.UNAUTHENTICATED.withDescription(e.toString()).asRuntimeException();
    }
  }
}
