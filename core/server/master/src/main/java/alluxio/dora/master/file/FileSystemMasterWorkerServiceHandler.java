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

package alluxio.dora.master.file;

import alluxio.dora.RpcUtils;
import alluxio.dora.grpc.FileSystemHeartbeatPOptions;
import alluxio.dora.grpc.FileSystemHeartbeatPRequest;
import alluxio.dora.grpc.FileSystemHeartbeatPResponse;
import alluxio.dora.grpc.FileSystemMasterWorkerServiceGrpc;
import alluxio.dora.grpc.GetFileInfoPOptions;
import alluxio.dora.grpc.GetFileInfoPRequest;
import alluxio.dora.grpc.GetFileInfoPResponse;
import alluxio.dora.grpc.GetPinnedFileIdsPOptions;
import alluxio.dora.grpc.GetPinnedFileIdsPRequest;
import alluxio.dora.grpc.GetPinnedFileIdsPResponse;
import alluxio.dora.grpc.GetUfsInfoPOptions;
import alluxio.dora.grpc.GetUfsInfoPRequest;
import alluxio.dora.grpc.GetUfsInfoPResponse;
import alluxio.dora.grpc.GrpcUtils;
import alluxio.dora.master.file.contexts.WorkerHeartbeatContext;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This class is a gRPC handler for file system master RPCs invoked by an Alluxio worker.
 */
public final class FileSystemMasterWorkerServiceHandler
    extends FileSystemMasterWorkerServiceGrpc.FileSystemMasterWorkerServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemMasterWorkerServiceHandler.class);

  private final FileSystemMaster mFileSystemMaster;

  /**
   * Creates a new instance of {@link FileSystemMasterWorkerServiceHandler}.
   *
   * @param fileSystemMaster the {@link FileSystemMaster} the handler uses internally
   */
  public FileSystemMasterWorkerServiceHandler(FileSystemMaster fileSystemMaster) {
    Preconditions.checkNotNull(fileSystemMaster, "fileSystemMaster");
    mFileSystemMaster = fileSystemMaster;
  }

  @Override
  public void fileSystemHeartbeat(FileSystemHeartbeatPRequest request,
      StreamObserver<FileSystemHeartbeatPResponse> responseObserver) {

    final long workerId = request.getWorkerId();
    final List<Long> persistedFiles = request.getPersistedFilesList();
    FileSystemHeartbeatPOptions options = request.getOptions();

    RpcUtils.call(LOG,
            () -> FileSystemHeartbeatPResponse
                .newBuilder()
                .setCommand(GrpcUtils.toProto(mFileSystemMaster.workerHeartbeat(workerId,
                    persistedFiles, WorkerHeartbeatContext.create(options.toBuilder()))))
                .build(),
        "workerHeartbeat", "workerId=%s, persistedFiles=%s, options=%s", responseObserver, workerId,
        persistedFiles, options);
  }

  @Override
  public void getFileInfo(GetFileInfoPRequest request,
      StreamObserver<GetFileInfoPResponse> responseObserver) {

    final long fileId = request.getFileId();
    GetFileInfoPOptions options = request.getOptions();

    RpcUtils.call(LOG,
            () -> GetFileInfoPResponse
                .newBuilder().setFileInfo(GrpcUtils.toProto(mFileSystemMaster.getFileInfo(fileId)))
                .build(),
        "getFileInfo", "fileId=%s, options=%s", responseObserver, fileId, options);
  }

  @Override
  public void getPinnedFileIds(GetPinnedFileIdsPRequest request,
      StreamObserver<GetPinnedFileIdsPResponse> responseObserver) {

    GetPinnedFileIdsPOptions options = request.getOptions();

    RpcUtils.call(LOG,
            () -> GetPinnedFileIdsPResponse
                .newBuilder().addAllPinnedFileIds(mFileSystemMaster.getPinIdList()).build(),
        "getPinnedFileIds", "options=%s", responseObserver, options);
  }

  @Override
  public void getUfsInfo(GetUfsInfoPRequest request,
      StreamObserver<GetUfsInfoPResponse> responseObserver) {

    final long mountId = request.getMountId();
    GetUfsInfoPOptions options = request.getOptions();

    RpcUtils.call(LOG,
            () -> GetUfsInfoPResponse
                .newBuilder().setUfsInfo(GrpcUtils.toProto(mFileSystemMaster.getUfsInfo(mountId)))
                .build(),
        "getUfsInfo", "mountId=%s, options=%s", responseObserver, mountId, options);
  }
}
