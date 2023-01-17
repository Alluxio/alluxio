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
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.ReadResponseMarshaller;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.worker.WorkerProcess;
import alluxio.worker.dora.DoraWorker;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import io.grpc.MethodDescriptor;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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

  private final LoadingCache<String, UfsFileStatus> mUfsFileStatusCache;

  /**
   * Creates a new implementation of gRPC BlockWorker interface.
   * @param workerProcess the worker process
   */
  public DoraWorkerClientServiceHandler(WorkerProcess workerProcess) {
    mWorker = workerProcess.getWorker(DoraWorker.class);
    UnderFileSystem ufs = UnderFileSystem.Factory.create(
        Configuration.getString(PropertyKey.DORA_CLIENT_UFS_ROOT),
        UnderFileSystemConfiguration.defaults(Configuration.global()));
    mUfsFileStatusCache = CacheBuilder.newBuilder()
        .maximumSize(Configuration.getInt(PropertyKey.DORA_UFS_FILE_STATUS_CACHE_SIZE))
        .expireAfterWrite(Configuration.getDuration(PropertyKey.DORA_UFS_FILE_STATUS_CACHE_TTL))
        .build(new CacheLoader<String, UfsFileStatus>() {
          @Override
          public UfsFileStatus load(String path) throws Exception {
            return ufs.getFileStatus(path);
          }
        });
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
      UfsFileStatus status = mUfsFileStatusCache.get(request.getPath());
      GetStatusPResponse response = GetStatusPResponse.newBuilder()
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
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (ExecutionException e) {
      LOG.error(String.format("Failed to get status of %s: ", request.getPath()), e);
      responseObserver.onError(e);
    }
  }
}
