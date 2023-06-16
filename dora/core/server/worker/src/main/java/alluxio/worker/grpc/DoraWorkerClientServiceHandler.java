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

import static java.util.Objects.requireNonNull;

import alluxio.RpcUtils;
import alluxio.annotation.SuppressFBWarnings;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.grpc.BlockWorkerGrpc;
import alluxio.grpc.CompleteFilePRequest;
import alluxio.grpc.CompleteFilePResponse;
import alluxio.grpc.CopyRequest;
import alluxio.grpc.CopyResponse;
import alluxio.grpc.CreateDirectoryPRequest;
import alluxio.grpc.CreateDirectoryPResponse;
import alluxio.grpc.CreateFilePRequest;
import alluxio.grpc.CreateFilePResponse;
import alluxio.grpc.DeletePRequest;
import alluxio.grpc.DeletePResponse;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.GetStatusPResponse;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.ListStatusPRequest;
import alluxio.grpc.ListStatusPResponse;
import alluxio.grpc.LoadFileFailure;
import alluxio.grpc.LoadFileRequest;
import alluxio.grpc.LoadFileResponse;
import alluxio.grpc.MoveRequest;
import alluxio.grpc.MoveResponse;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.ReadResponseMarshaller;
import alluxio.grpc.RenamePRequest;
import alluxio.grpc.RenamePResponse;
import alluxio.grpc.RouteFailure;
import alluxio.grpc.TaskStatus;
import alluxio.underfs.UfsStatus;
import alluxio.util.io.PathUtils;
import alluxio.worker.dora.DoraWorker;
import alluxio.worker.dora.OpenFileHandle;
import alluxio.worker.dora.PagedDoraWorker;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.grpc.MethodDescriptor;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Server side implementation of the gRPC dora worker interface.
 */
@SuppressFBWarnings("BC_UNCONFIRMED_CAST")
public class DoraWorkerClientServiceHandler extends BlockWorkerGrpc.BlockWorkerImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(DoraWorkerClientServiceHandler.class);

  private static final boolean ZERO_COPY_ENABLED =
      Configuration.getBoolean(PropertyKey.WORKER_NETWORK_ZEROCOPY_ENABLED);
  private static final int LIST_STATUS_BATCH_SIZE =
      Configuration.getInt(PropertyKey.MASTER_FILE_SYSTEM_LISTSTATUS_RESULTS_PER_MESSAGE);

  private final ReadResponseMarshaller mReadResponseMarshaller = new ReadResponseMarshaller();
  private final DoraWorker mWorker;

  /**
   * Creates a new implementation of gRPC BlockWorker interface.
   * @param doraWorker the DoraWorker object
   */
  @Inject
  public DoraWorkerClientServiceHandler(DoraWorker doraWorker) {
    mWorker = requireNonNull(doraWorker);
  }

  /**
   * @return a map of gRPC methods with overridden descriptors
   */
  public Map<MethodDescriptor, MethodDescriptor> getOverriddenMethodDescriptors() {
    if (ZERO_COPY_ENABLED) {
      return ImmutableMap.of(
          BlockWorkerGrpc.getReadBlockMethod(),
          BlockWorkerGrpc.getReadBlockMethod().toBuilder()
              .setResponseMarshaller(mReadResponseMarshaller).build()
      );
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
    FileReadHandler readHandler = new FileReadHandler(GrpcExecutors.BLOCK_READER_EXECUTOR,
        mWorker, callStreamObserver);
    callStreamObserver.setOnReadyHandler(readHandler::onReady);
    return readHandler;
  }

  @Override
  public void loadFile(LoadFileRequest request, StreamObserver<LoadFileResponse> responseObserver) {
    try {
      ListenableFuture<List<LoadFileFailure>> failures =
          mWorker.load(!request.getLoadMetadataOnly(), request.getUfsStatusList().stream().map(
              UfsStatus::fromProto).collect(
              Collectors.toList()), request.getOptions());
      ListenableFuture<LoadFileResponse> future = Futures.transform(failures, fail -> {
        int numFiles = request.getUfsStatusCount();
        TaskStatus taskStatus = TaskStatus.SUCCESS;
        if (fail.size() > 0) {
          taskStatus = numFiles > fail.size() ? TaskStatus.PARTIAL_FAILURE : TaskStatus.FAILURE;
        }
        LoadFileResponse.Builder response = LoadFileResponse.newBuilder();
        return response.addAllFailures(fail).setStatus(taskStatus).build();
      }, GrpcExecutors.BLOCK_WRITER_EXECUTOR);
      RpcUtils.invoke(LOG, future, "loadFile", "request=%s", responseObserver, request);
    } catch (Exception e) {
      LOG.debug(String.format("Failed to load file %s: ", request.getUfsStatusList()), e);
      responseObserver.onError(AlluxioRuntimeException.from(e).toGrpcStatusRuntimeException());
    }
  }

  @Override
  public void copy(CopyRequest request, StreamObserver<CopyResponse> responseObserver) {
    try {
      ListenableFuture<List<RouteFailure>> failures =
          mWorker.copy(request.getRoutesList(), request.getUfsReadOptions(),
              request.getWriteOptions());
      ListenableFuture<CopyResponse> future = Futures.transform(failures, fail -> {
        int numFiles = request.getRoutesCount();
        TaskStatus taskStatus = TaskStatus.SUCCESS;
        if (fail.size() > 0) {
          taskStatus = numFiles > fail.size() ? TaskStatus.PARTIAL_FAILURE : TaskStatus.FAILURE;
        }
        CopyResponse.Builder response = CopyResponse.newBuilder();
        return response.addAllFailures(fail).setStatus(taskStatus).build();
      }, GrpcExecutors.BLOCK_WRITER_EXECUTOR);
      RpcUtils.invoke(LOG, future, "loadFile", "request=%s", responseObserver, request);
    } catch (Exception e) {
      LOG.debug(String.format("Failed to load file %s: ", request.getRoutesList()), e);
      responseObserver.onError(AlluxioRuntimeException.from(e).toGrpcStatusRuntimeException());
    }
  }

  @Override
  public void move(MoveRequest request, StreamObserver<MoveResponse> responseObserver) {
    try {
      ListenableFuture<List<RouteFailure>> failures =
              mWorker.move(request.getRoutesList(), request.getUfsReadOptions(),
                      request.getWriteOptions());
      ListenableFuture<MoveResponse> future = Futures.transform(failures, fail -> {
        int numFiles = request.getRoutesCount();
        TaskStatus taskStatus = TaskStatus.SUCCESS;
        if (fail.size() > 0) {
          taskStatus = numFiles > fail.size() ? TaskStatus.PARTIAL_FAILURE : TaskStatus.FAILURE;
        }
        MoveResponse.Builder response = MoveResponse.newBuilder();
        return response.addAllFailures(fail).setStatus(taskStatus).build();
      }, GrpcExecutors.BLOCK_WRITER_EXECUTOR);
      RpcUtils.invoke(LOG, future, "moveFile", "request=%s", responseObserver, request);
    } catch (Exception e) {
      LOG.debug(String.format("Failed to move file %s: ", request.getRoutesList()), e);
      responseObserver.onError(AlluxioRuntimeException.from(e).toGrpcStatusRuntimeException());
    }
  }

  @Override
  public void getStatus(GetStatusPRequest request,
      StreamObserver<GetStatusPResponse> responseObserver) {
    try {
      alluxio.wire.FileInfo fileInfo = mWorker.getFileInfo(request.getPath(),
          request.getOptions());
      GetStatusPResponse response =
          GetStatusPResponse.newBuilder()
              .setFileInfo(GrpcUtils.toProto(fileInfo))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (IOException | AccessControlException e) {
      LOG.debug(String.format("Failed to get status of %s: ", request.getPath()), e);
      responseObserver.onError(AlluxioRuntimeException.from(e).toGrpcStatusRuntimeException());
    }
  }

  @Override
  public void listStatus(ListStatusPRequest request,
                         StreamObserver<ListStatusPResponse> responseObserver) {
    LOG.debug("listStatus is called for {}", request.getPath());

    try {
      UfsStatus[] statuses = mWorker.listStatus(request.getPath(), request.getOptions());
      if (statuses == null) {
        responseObserver.onError(
            new NotFoundRuntimeException(String.format("%s Not Found", request.getPath()))
                .toGrpcStatusRuntimeException());
        return;
      }

      ListStatusPResponse.Builder builder = ListStatusPResponse.newBuilder();

      for (int i = 0; i < statuses.length; i++) {
        UfsStatus status = statuses[i];
        String ufsFullPath = PathUtils.concatPath(request.getPath(), status.getName());

        alluxio.grpc.FileInfo fi =
            ((PagedDoraWorker) mWorker).buildFileInfoFromUfsStatus(status, ufsFullPath);

        builder.addFileInfos(fi);
        if (builder.getFileInfosCount() == LIST_STATUS_BATCH_SIZE) {
          // Reached the batch size of the reply message. Send it out and create a new one.
          responseObserver.onNext(builder.build());
          builder = ListStatusPResponse.newBuilder();
        }
      }
      if (builder.getFileInfosCount() != 0) {
        // Send out the remaining items if there is any.
        responseObserver.onNext(builder.build());
      }

      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error(String.format("Failed to list status of %s: ", request.getPath()), e);
      responseObserver.onError(AlluxioRuntimeException.from(e).toGrpcStatusRuntimeException());
    }
  }

  @Override
  public void createFile(CreateFilePRequest request,
                         StreamObserver<CreateFilePResponse> responseObserver) {
    LOG.debug("Got createFile: {}", request);
    try {
      String ufsFullPath = request.getPath();

      OpenFileHandle handle = mWorker.createFile(ufsFullPath, request.getOptions());

      CreateFilePResponse response = CreateFilePResponse.newBuilder()
          .setFileInfo(handle.getInfo())
          .setUuid(handle.getUUID().toString())
          .build();

      // We return the UUID of the handle to client, and verify the handle for each
      // upcoming/subsequent write request.
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error(String.format("Failed to create file for %s: ", request.getPath()), e);
      responseObserver.onError(AlluxioRuntimeException.from(e).toGrpcStatusRuntimeException());
    }
  }

  @Override
  public void completeFile(CompleteFilePRequest request,
                           StreamObserver<CompleteFilePResponse> responseObserver) {
    LOG.debug("Got completeFile: {}", request);
    try {
      String ufsFullPath = request.getPath();

      mWorker.completeFile(ufsFullPath, request.getOptions(), request.getUuid());
      CompleteFilePResponse response = CompleteFilePResponse.newBuilder().build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error(String.format("Failed to complete file for %s: ", request.getPath()), e);
      responseObserver.onError(AlluxioRuntimeException.from(e).toGrpcStatusRuntimeException());
    }
  }

  @Override
  public void remove(DeletePRequest request, StreamObserver<DeletePResponse> responseObserver) {
    LOG.debug("Got Remove: {}", request);
    try {
      String ufsFullPath = request.getPath();

      mWorker.delete(ufsFullPath, request.getOptions());
      DeletePResponse response = DeletePResponse.newBuilder().build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error(String.format("Failed to delete file for %s: ", request.getPath()), e);
      responseObserver.onError(AlluxioRuntimeException.from(e).toGrpcStatusRuntimeException());
    }
  }

  @Override
  public void rename(RenamePRequest request, StreamObserver<RenamePResponse> responseObserver) {
    LOG.debug("Got rename: {}", request);
    String src = request.getPath();
    String dst = request.getDstPath();
    try {
      mWorker.rename(src, dst, request.getOptions());
      RenamePResponse response = RenamePResponse.newBuilder().build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error(String.format("Failed to rename file for %s -> %s: ", src, dst), e);
      responseObserver.onError(AlluxioRuntimeException.from(e).toGrpcStatusRuntimeException());
    }
  }

  @Override
  public void createDirectory(CreateDirectoryPRequest request,
                              StreamObserver<CreateDirectoryPResponse> responseObserver) {
    LOG.debug("Got CreateDirectory: {}", request);
    try {
      String ufsFullPath = request.getPath();

      mWorker.createDirectory(ufsFullPath, request.getOptions());
      CreateDirectoryPResponse response = CreateDirectoryPResponse.newBuilder().build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error(String.format("Failed to CreateDirectory for %s: ", request.getPath()), e);
      responseObserver.onError(AlluxioRuntimeException.from(e).toGrpcStatusRuntimeException());
    }
  }
}
