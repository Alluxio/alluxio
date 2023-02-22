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
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.GetStatusPResponse;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.ReadResponseMarshaller;
<<<<<<< HEAD
||||||| 047e094360
import alluxio.proto.meta.DoraMeta;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;
=======
import alluxio.proto.meta.DoraMeta;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.UnderFileSystemUtils;
>>>>>>> d154718e3611c17546896bcb1fb6a3cf9d40e7e5
import alluxio.worker.WorkerProcess;
import alluxio.worker.dora.DoraWorker;

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
<<<<<<< HEAD
      alluxio.wire.FileInfo fileInfo = mWorker.getFileInfo(request.getPath());
      GetStatusPResponse response =
          GetStatusPResponse.newBuilder()
              .setFileInfo(GrpcUtils.toProto(fileInfo))
              .build();
||||||| 047e094360
      String alluxioFilePath = request.getPath();

      String ufsFullPath = PathUtils.concatPath(mRootUFS, alluxioFilePath);
      String fn = new AlluxioURI(alluxioFilePath).getName();

      UfsStatus status = mUfsStatusCache.getIfPresent(ufsFullPath);
      if (status == null) {
        // The requested FileStatus is not present in memory cache.
        // Let's try to query local persistent DoraMetaStore.
        DoraMetaStore doraMetaStore = ((PagedDoraWorker) mWorker).getMetaStore();
        Optional<DoraMeta.FileStatus> fs = doraMetaStore.getDoraMeta(ufsFullPath);
        if (fs.isPresent()) {
          // Found in persistent DoraMetaStore
          fi = fs.get().getFileInfo();
          String contentHash = UnderFileSystemUtils.approximateContentHash(fi.getLength(),
              fi.getLastModificationTimeMs());
          UfsFileStatus ufs = new UfsFileStatus(fi.getPath(), contentHash, fi.getLength(),
              fi.getLastModificationTimeMs(),
              fi.getOwner(), fi.getGroup(), (short) fi.getMode(), fi.getBlockSizeBytes());
          mUfsStatusCache.put(ufsFullPath, ufs);
        } else {
          // This will load UfsFileStatus from UFS and put it in memory cache
          status = mUfsStatusCache.get(ufsFullPath);
          fi = buildFileInfoFromUfsStatus(status, fn, alluxioFilePath, ufsFullPath);

          // Add this to persistent DoraMetaStore.
          long currentTimeMillis = System.currentTimeMillis();
          doraMetaStore.putDoraMeta(ufsFullPath,
              DoraMeta.FileStatus.newBuilder().setFileInfo(fi).setTs(currentTimeMillis).build());
        }
      } else {
        // Found in memory cache
        fi = buildFileInfoFromUfsStatus(status, fn, alluxioFilePath, ufsFullPath);
      }

      GetStatusPResponse response = GetStatusPResponse.newBuilder().setFileInfo(fi).build();
=======
      String alluxioFilePath = request.getPath();

      String ufsFullPath = alluxioFilePath; // Now the full UFS path is passed from client.
      String fn = new AlluxioURI(alluxioFilePath).getName();

      UfsStatus status = mUfsStatusCache.getIfPresent(ufsFullPath);
      if (status == null) {
        // The requested FileStatus is not present in memory cache.
        // Let's try to query local persistent DoraMetaStore.
        DoraMetaStore doraMetaStore = ((PagedDoraWorker) mWorker).getMetaStore();
        Optional<DoraMeta.FileStatus> fs = doraMetaStore.getDoraMeta(ufsFullPath);
        if (fs.isPresent()) {
          // Found in persistent DoraMetaStore
          fi = fs.get().getFileInfo();
          String contentHash = UnderFileSystemUtils.approximateContentHash(fi.getLength(),
              fi.getLastModificationTimeMs());
          UfsFileStatus ufs = new UfsFileStatus(fi.getPath(), contentHash, fi.getLength(),
              fi.getLastModificationTimeMs(),
              fi.getOwner(), fi.getGroup(), (short) fi.getMode(), fi.getBlockSizeBytes());
          mUfsStatusCache.put(ufsFullPath, ufs);
        } else {
          // This will load UfsFileStatus from UFS and put it in memory cache
          status = mUfsStatusCache.get(ufsFullPath);
          fi = buildFileInfoFromUfsStatus(status, fn, alluxioFilePath, ufsFullPath);

          // Add this to persistent DoraMetaStore.
          long currentTimeMillis = System.currentTimeMillis();
          doraMetaStore.putDoraMeta(ufsFullPath,
              DoraMeta.FileStatus.newBuilder().setFileInfo(fi).setTs(currentTimeMillis).build());
        }
      } else {
        // Found in memory cache
        fi = buildFileInfoFromUfsStatus(status, fn, alluxioFilePath, ufsFullPath);
      }

      GetStatusPResponse response = GetStatusPResponse.newBuilder().setFileInfo(fi).build();
>>>>>>> d154718e3611c17546896bcb1fb6a3cf9d40e7e5
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (IOException e) {
      LOG.error(String.format("Failed to get status of %s: ", request.getPath()), e);
      responseObserver.onError(e);
    }
  }
}
