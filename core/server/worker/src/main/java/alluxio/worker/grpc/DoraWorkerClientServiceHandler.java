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

import alluxio.annotation.SuppressFBWarnings;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.grpc.BlockWorkerGrpc;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.GetStatusPResponse;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.ListStatusPRequest;
import alluxio.grpc.ListStatusPResponse;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.ReadResponseMarshaller;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.options.ListOptions;
import alluxio.worker.WorkerProcess;
import alluxio.worker.dora.DoraWorker;
import alluxio.worker.dora.PagedDoraWorker;

import com.google.common.collect.ImmutableMap;
import io.grpc.MethodDescriptor;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

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
   * @param workerProcess the worker process
   */
  public DoraWorkerClientServiceHandler(WorkerProcess workerProcess) {
    mWorker = workerProcess.getWorker(DoraWorker.class);
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
  public void getStatus(GetStatusPRequest request,
      StreamObserver<GetStatusPResponse> responseObserver) {
    try {
      alluxio.wire.FileInfo fileInfo = mWorker.getFileInfo(request.getPath(), request.getOptions());
      GetStatusPResponse response =
          GetStatusPResponse.newBuilder()
              .setFileInfo(GrpcUtils.toProto(fileInfo))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (IOException e) {
      LOG.debug(String.format("Failed to get status of %s: ", request.getPath()), e);
      responseObserver.onError(AlluxioRuntimeException.from(e).toGrpcStatusRuntimeException());
    }
  }

  @Override
  public void listStatus(ListStatusPRequest request,
                         StreamObserver<ListStatusPResponse> responseObserver) {
    LOG.debug("listStatus is called for {}", request.getPath());

    try {
      ListOptions options = ListOptions.defaults().setRecursive(
          request.getOptions().hasRecursive() ? request.getOptions().getRecursive() : false);
      UfsStatus[] statuses = mWorker.listStatus(request.getPath(), options);
      if (statuses == null) {
        responseObserver.onError(
            new NotFoundRuntimeException(String.format("%s Not Found", request.getPath()))
                .toGrpcStatusRuntimeException());
        return;
      }

      ListStatusPResponse.Builder builder = ListStatusPResponse.newBuilder();

      for (int i = 0; i < statuses.length; i++) {
        UfsStatus status = statuses[i];
        String ufsFullPath = status.getName();
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
}
