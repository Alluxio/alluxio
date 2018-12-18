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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.grpc.AsyncCacheRequest;
import alluxio.grpc.AsyncCacheResponse;
import alluxio.grpc.BlockWorkerGrpc;
import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.RemoveBlockRequest;
import alluxio.grpc.RemoveBlockResponse;
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteResponse;
import alluxio.util.IdUtils;
import alluxio.util.RpcUtilsNew;
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.AsyncCacheRequestManager;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Preconditions;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server side implementation of the gRPC BlockWorker interface.
 */
public class BlockWorkerImpl extends BlockWorkerGrpc.BlockWorkerImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(BlockWorkerImpl.class);

  private WorkerProcess mWorkerProcess;
  private FileTransferType mFileTransferType;
  private final AsyncCacheRequestManager mRequestManager;

  /**
   * Creates a new implementation of gRPC BlockWorker interface.
   *
   * @param workerProcess the worker process
   */
  public BlockWorkerImpl(WorkerProcess workerProcess) {
    mWorkerProcess = workerProcess;
    mFileTransferType = Configuration
        .getEnum(PropertyKey.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE, FileTransferType.class);
    mRequestManager = new AsyncCacheRequestManager(
        GrpcExecutors.ASYNC_CACHE_MANAGER_EXECUTOR, mWorkerProcess.getWorker(BlockWorker.class));
  }

  @Override
  public void readBlock(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
    final ServerCallStreamObserver<ReadResponse> serverCallStreamObserver =
        (ServerCallStreamObserver<ReadResponse>) responseObserver;
    BlockReadHandler readHandler = new BlockReadHandler(GrpcExecutors.BLOCK_READER_EXECUTOR,
        mWorkerProcess.getWorker(BlockWorker.class), mFileTransferType);
    serverCallStreamObserver.setOnReadyHandler(readHandler::onReady);
    serverCallStreamObserver.setOnCancelHandler(readHandler::onCancel);
    try {
      readHandler.readBlock(request, responseObserver);
    } catch (Exception e) {
      readHandler.onError(e);
      responseObserver.onError(e);
    }
  }

  @Override
  public StreamObserver<alluxio.grpc.WriteRequest> writeBlock(final StreamObserver<WriteResponse> responseObserver) {
    WriteRequestStreamObserver requestObserver = new WriteRequestStreamObserver(responseObserver);
    ServerCallStreamObserver<WriteResponse> serverResponseObserver =
        (ServerCallStreamObserver<WriteResponse>) responseObserver;
    serverResponseObserver.setOnCancelHandler(requestObserver::onCancel);
    return requestObserver;
  }

  private class WriteRequestStreamObserver implements StreamObserver<alluxio.grpc.WriteRequest> {
    private final StreamObserver<WriteResponse> mResponseObserver;
    private AbstractWriteHandler mWriteHandler;

    public WriteRequestStreamObserver(StreamObserver<WriteResponse> responseObserver) {
      mResponseObserver = responseObserver;
    }

    private AbstractWriteHandler createWriterHandler(WriteRequest request) {
      switch (request.getCommand().getType()) {
        case ALLUXIO_BLOCK:
          return new BlockWriteHandler(mWorkerProcess.getWorker(BlockWorker.class),
              mResponseObserver);
        case UFS_FILE:
          return new UfsFileWriteHandler(mWorkerProcess.getUfsManager(),
              mResponseObserver);
        case UFS_FALLBACK_BLOCK:
          return new UfsFallbackBlockWriteHandler(
              mWorkerProcess.getWorker(BlockWorker.class), mWorkerProcess.getUfsManager(),
              mResponseObserver);
        default:
          throw new IllegalArgumentException(String.format("Invalid request type %s",
              request.getCommand().getType().name()));
      }
    }

    @Override
    public void onNext(WriteRequest request) {
      try {
        if (mWriteHandler == null) {
          mWriteHandler = createWriterHandler(request);
        }
        mWriteHandler.write(request);
      } catch(Exception e) {
        mWriteHandler.onError(e);
      }
    }

    @Override
    public void onError(Throwable t) {
      if (mWriteHandler == null) {
        // close the response stream if the client error before sending a write request
        mResponseObserver.onCompleted();
      } else {
        mWriteHandler.onError(t);
      }
    }

    @Override
    public void onCompleted() {
      try {
        mWriteHandler.onComplete();
      } catch (Exception e) {
        mResponseObserver.onError(e);
      }
    }

    /**
     * Handles cancel event from the client.
     */
    public void onCancel() {
      if (mWriteHandler != null) {
        mWriteHandler.onCancel();
      }
    }
  }

  @Override
  public void openLocalBlock(OpenLocalBlockRequest request,
      StreamObserver<OpenLocalBlockResponse> responseObserver) {
    final ServerCallStreamObserver<OpenLocalBlockResponse> serverCallStreamObserver =
        (ServerCallStreamObserver<OpenLocalBlockResponse>) responseObserver;
    ShortCircuitBlockReadHandler handler = new ShortCircuitBlockReadHandler(
        mWorkerProcess.getWorker(BlockWorker.class), request, responseObserver);
    serverCallStreamObserver.setOnCancelHandler(() -> {
      Preconditions.checkState(Context.current().isCancelled());
      Status status = Contexts.statusFromCancelled(Context.current());
      if (status.getCode() == Status.Code.CANCELLED) {
        handler.handleBlockCloseRequest();
      } else {
        handler.exceptionCaught(status.getCause());
      }
    });
    handler.handleBlockOpenRequest();
  }

  @Override
  public void createLocalBlock(CreateLocalBlockRequest request,
      StreamObserver<CreateLocalBlockResponse> responseObserver) {
    final ServerCallStreamObserver<CreateLocalBlockResponse> serverCallStreamObserver =
        (ServerCallStreamObserver<CreateLocalBlockResponse>) responseObserver;
    ShortCircuitBlockWriteHandler handler = new ShortCircuitBlockWriteHandler(
        mWorkerProcess.getWorker(BlockWorker.class), request, responseObserver);
    serverCallStreamObserver.setOnCancelHandler(() -> {
      Preconditions.checkState(Context.current().isCancelled());
      Status status = Contexts.statusFromCancelled(Context.current());
      if (status.getCode() == Status.Code.CANCELLED) {
        handler.handleBlockCompleteRequest(Context.current().cancellationCause() != null);
      } else {
        handler.exceptionCaught(status.getCause());
      }
    });
    handler.handleBlockCreateRequest();
  }

  @Override
  public void asyncCache(AsyncCacheRequest request,
      StreamObserver<AsyncCacheResponse> responseObserver) {
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<AsyncCacheResponse>) () -> {
      mRequestManager.submitRequest(request);
      return AsyncCacheResponse.getDefaultInstance();
    }, "asyncCache", "request=%s", responseObserver, request);
  }

  @Override
  public void removeBlock(RemoveBlockRequest request,
      StreamObserver<RemoveBlockResponse> responseObserver) {
    final long sessionId = IdUtils.createSessionId();
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<RemoveBlockResponse>) () -> {
      mWorkerProcess.getWorker(BlockWorker.class).removeBlock(sessionId, request.getBlockId());
      return RemoveBlockResponse.getDefaultInstance();
    }, "removeBlock", "request=%s", responseObserver, request);
  }
}
