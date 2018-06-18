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

import alluxio.AlluxioURI;
import alluxio.file.FileSystemMasterOptionsService;
import alluxio.util.RpcUtilsNew;
import alluxio.exception.AlluxioException;
import alluxio.grpc.FileSystemMasterServiceGrpc;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.GetStatusPResponse;
import alluxio.file.FileSystemMasterService;
import alluxio.util.grpc.GrpcUtils;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 * This class is a gRPC handler for file system master RPCs invoked by an Alluxio client.
 */
public final class FileSystemMasterClientServiceHandlerNew
    extends FileSystemMasterServiceGrpc.FileSystemMasterServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemMasterClientServiceHandlerNew.class);
  private final FileSystemMasterService mFileSystemMaster;
  private final FileSystemMasterOptionsService mOptionsService;

  /**
   * Creates a new instance of {@link FileSystemMasterClientServiceHandlerNew}.
   *
   * @param fileSystemMaster the {@link FileSystemMaster} the handler uses internally
   */
  public FileSystemMasterClientServiceHandlerNew(FileSystemMasterService fileSystemMaster) {
    Preconditions.checkNotNull(fileSystemMaster, "fileSystemMaster");
    mFileSystemMaster = fileSystemMaster;
    mOptionsService = fileSystemMaster.optionsService();
  }

  @Override
  public void getStatus(alluxio.grpc.GetStatusPRequest request,
      StreamObserver<GetStatusPResponse> responseObserver) {
   String path = request.getPath();
   GetStatusPOptions options = request.getOptions();
   RpcUtilsNew.call(LOG, new RpcUtilsNew.RpcCallableThrowsIOException<GetStatusPResponse>() {
      @Override
      public GetStatusPResponse call() throws AlluxioException, IOException {
        return GetStatusPResponse
            .newBuilder().setFileInfo(GrpcUtils.toProto(mFileSystemMaster
                .getFileInfo(new AlluxioURI(path), GrpcUtils.fromProto(mOptionsService, options))))
            .build();
      }

      @Override
      public String toString() {
        return String.format("GetStatus: path=%s, options=%s", path, options);
      }
      // getStatus is often used to check file existence, so we avoid logging all of its failures
    }, responseObserver, false);

  }
}
