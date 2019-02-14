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

package alluxio.master.file;

import alluxio.RpcUtils;
import alluxio.grpc.FileSystemMasterJobServiceGrpc;
import alluxio.grpc.GetFileInfoPOptions;
import alluxio.grpc.GetFileInfoPRequest;
import alluxio.grpc.GetFileInfoPResponse;
import alluxio.grpc.GetUfsInfoPOptions;
import alluxio.grpc.GetUfsInfoPRequest;
import alluxio.grpc.GetUfsInfoPResponse;
import alluxio.grpc.GrpcUtils;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a gRPC handler for file system master RPCs invoked by Alluxio job service.
 */
public final class FileSystemMasterJobServiceHandler
    extends FileSystemMasterJobServiceGrpc.FileSystemMasterJobServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemMasterJobServiceHandler.class);

  private final FileSystemMaster mFileSystemMaster;

  /**
   * Creates a new instance of {@link FileSystemMasterJobServiceHandler}.
   *
   * @param fileSystemMaster the {@link FileSystemMaster} the handler uses internally
   */
  public FileSystemMasterJobServiceHandler(FileSystemMaster fileSystemMaster) {
    Preconditions.checkNotNull(fileSystemMaster);
    mFileSystemMaster = fileSystemMaster;
  }

  @Override
  public void getFileInfo(GetFileInfoPRequest request,
      StreamObserver<GetFileInfoPResponse> responseObserver) {

    final long fileId = request.getFileId();
    GetFileInfoPOptions options = request.getOptions();

    RpcUtils.call(LOG,
        (RpcUtils.RpcCallableThrowsIOException<GetFileInfoPResponse>) () -> GetFileInfoPResponse
            .newBuilder().setFileInfo(GrpcUtils.toProto(mFileSystemMaster.getFileInfo(fileId)))
            .build(),
        "getFileInfo", "fileId=%s, options=%s", responseObserver, fileId, options);
  }

  @Override
  public void getUfsInfo(GetUfsInfoPRequest request,
      StreamObserver<GetUfsInfoPResponse> responseObserver) {

    final long mountId = request.getMountId();
    GetUfsInfoPOptions options = request.getOptions();

    RpcUtils.call(LOG,
        (RpcUtils.RpcCallableThrowsIOException<GetUfsInfoPResponse>) () -> GetUfsInfoPResponse
            .newBuilder().setUfsInfo(GrpcUtils.toProto(mFileSystemMaster.getUfsInfo(mountId)))
            .build(),
        "getUfsInfo", "mountId=%s, options=%s", responseObserver, mountId, options);
  }
}
