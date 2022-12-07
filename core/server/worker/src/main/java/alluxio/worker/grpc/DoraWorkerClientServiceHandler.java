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
import alluxio.grpc.BlockWorkerGrpc;
import alluxio.grpc.FileInfo;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.GetStatusPResponse;
import alluxio.grpc.PAcl;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.ReadResponseMarshaller;
import alluxio.grpc.TtlAction;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.worker.DoraWorker;
import alluxio.worker.Worker;
import alluxio.worker.WorkerProcess;

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

  private final ReadResponseMarshaller mReadResponseMarshaller = new ReadResponseMarshaller();
  private final DoraWorker mWorker;

  /**
   * Creates a new implementation of gRPC BlockWorker interface.
   *
   * @param workerProcess the worker process
   */
  public DoraWorkerClientServiceHandler(WorkerProcess workerProcess) {
    mWorker = (DoraWorker) workerProcess.getWorker(Worker.class);
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
      String ufsFilePath = request.getPath();
      UnderFileSystem ufs = UnderFileSystem.Factory.create(request.getPath(),
          UnderFileSystemConfiguration.defaults(Configuration.global()));
      UfsFileStatus status = ufs.getFileStatus(request.getPath());
      if (status == null) {
        LOG.info(String.format("Unable to get status for under file system path %s. ",
            ufsFilePath));
        throw new IOException(String.format("Unable to get status for under file system path %s. ",
            ufsFilePath));
      }
      GetStatusPResponse mResponse = GetStatusPResponse.newBuilder()
          .setFileInfo(
              FileInfo.newBuilder()
                  .setName(status.getName())
                  .setPath(ufsFilePath)
                  .setUfsPath(ufsFilePath)
                  .setLength(status.getContentLength())
                  .setBlockSizeBytes(status.getBlockSize())
                  .setMode(status.getMode())
                  .setFolder(status.isDirectory())
                  .setLastModificationTimeMs(status.getLastModifiedTime())
                  .setOwner(status.getOwner())
                  .setGroup(status.getGroup())
                  .build()
          ).build();
      responseObserver.onNext(mResponse);
      responseObserver.onCompleted();
    } catch (IOException e) {
      LOG.error(String.format("Failed to get status of %s: ", request.getPath()), e);
      responseObserver.onError(e);
    }
  }
}
