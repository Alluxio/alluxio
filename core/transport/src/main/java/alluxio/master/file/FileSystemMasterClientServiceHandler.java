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
import alluxio.exception.AlluxioException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.CheckConsistencyPOptions;
import alluxio.grpc.CheckConsistencyPRequest;
import alluxio.grpc.CheckConsistencyPResponse;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CompleteFilePRequest;
import alluxio.grpc.CompleteFilePResponse;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateDirectoryPRequest;
import alluxio.grpc.CreateDirectoryPResponse;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.CreateFilePRequest;
import alluxio.grpc.CreateFilePResponse;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.DeletePRequest;
import alluxio.grpc.DeletePResponse;
import alluxio.grpc.FileInfo;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.FreePRequest;
import alluxio.grpc.FreePResponse;
import alluxio.grpc.GetMountTablePRequest;
import alluxio.grpc.GetMountTablePResponse;
import alluxio.grpc.GetNewBlockIdForFilePOptions;
import alluxio.grpc.GetNewBlockIdForFilePRequest;
import alluxio.grpc.GetNewBlockIdForFilePResponse;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.ListStatusPRequest;
import alluxio.grpc.ListStatusPResponse;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.MountPRequest;
import alluxio.grpc.MountPResponse;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.RenamePRequest;
import alluxio.grpc.RenamePResponse;
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.grpc.ScheduleAsyncPersistencePRequest;
import alluxio.grpc.ScheduleAsyncPersistencePResponse;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.SetAttributePRequest;
import alluxio.grpc.SetAttributePResponse;
import alluxio.grpc.UnmountPOptions;
import alluxio.grpc.UnmountPRequest;
import alluxio.grpc.UnmountPResponse;
import alluxio.grpc.UpdateUfsModePOptions;
import alluxio.grpc.UpdateUfsModePRequest;
import alluxio.grpc.UpdateUfsModePResponse;
import alluxio.grpc.FileSystemMasterServiceGrpc;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.GetStatusPResponse;
import alluxio.underfs.UfsMode;
import alluxio.util.RpcUtilsNew;
import alluxio.util.grpc.GrpcUtils;
import alluxio.wire.MountPointInfo;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is a gRPC handler for file system master RPCs invoked by an Alluxio client.
 */
public final class FileSystemMasterClientServiceHandler
    extends FileSystemMasterServiceGrpc.FileSystemMasterServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemMasterClientServiceHandler.class);
  private final FileSystemMasterService mFileSystemMaster;
  private final FileSystemMasterOptions mOptionsService;

  /**
   * Creates a new instance of {@link FileSystemMasterClientServiceHandler}.
   *
   * @param fileSystemMaster the {@link FileSystemMaster} the handler uses internally
   */
  public FileSystemMasterClientServiceHandler(FileSystemMasterService fileSystemMaster) {
    Preconditions.checkNotNull(fileSystemMaster, "fileSystemMaster");
    mFileSystemMaster = fileSystemMaster;
    mOptionsService = fileSystemMaster.getMasterOptions();
  }

  @Override
  public void checkConsistency(CheckConsistencyPRequest request,
      StreamObserver<CheckConsistencyPResponse> responseObserver) {
    String path = request.getPath();
    CheckConsistencyPOptions options = request.getOptions();
    RpcUtilsNew.call(LOG,
        new RpcUtilsNew.RpcCallableThrowsIOException<CheckConsistencyPResponse>() {
          @Override
          public CheckConsistencyPResponse call() throws AlluxioException, IOException {
            List<AlluxioURI> inconsistentUris = mFileSystemMaster.checkConsistency(
                new AlluxioURI(path), GrpcUtils.fromProto(mOptionsService, options));
            List<String> uris = new ArrayList<>(inconsistentUris.size());
            for (AlluxioURI uri : inconsistentUris) {
              uris.add(uri.getPath());
            }
            return CheckConsistencyPResponse.newBuilder().addAllInconsistentPaths(uris).build();
          }

          @Override
          public String toString() {
            return String.format("CheckConsistency: path=%s, options=%s", path, options);
          }
        }, responseObserver);
  }

  @Override
  public void completeFile(CompleteFilePRequest request,
      StreamObserver<CompleteFilePResponse> responseObserver) {
    String path = request.getPath();
    CompleteFilePOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, new RpcUtilsNew.RpcCallableThrowsIOException<CompleteFilePResponse>() {
      @Override
      public CompleteFilePResponse call() throws AlluxioException, AlluxioStatusException {
        mFileSystemMaster.completeFile(new AlluxioURI(path),
            GrpcUtils.fromProto(mOptionsService, options));
        return CompleteFilePResponse.newBuilder().build();
      }

      @Override
      public String toString() {
        return String.format("CompleteFile: path=%s, options=%s", path, options);
      }
    }, responseObserver);
  }

  @Override
  public void createDirectory(CreateDirectoryPRequest request,
      StreamObserver<CreateDirectoryPResponse> responseObserver) {
    String path = request.getPath();
    CreateDirectoryPOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, new RpcUtilsNew.RpcCallableThrowsIOException<CreateDirectoryPResponse>() {
      @Override
      public CreateDirectoryPResponse call() throws AlluxioException, IOException {
        mFileSystemMaster.createDirectory(new AlluxioURI(path),
            GrpcUtils.fromProto(mOptionsService, options));
        return CreateDirectoryPResponse.newBuilder().build();
      }

      @Override
      public String toString() {
        return String.format("CreateDirectory: path=%s, options=%s", path, options);
      }
    }, responseObserver);
  }

  @Override
  public void createFile(CreateFilePRequest request,
      StreamObserver<CreateFilePResponse> responseObserver) {
    String path = request.getPath();
    CreateFilePOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, new RpcUtilsNew.RpcCallableThrowsIOException<CreateFilePResponse>() {
      @Override
      public CreateFilePResponse call() throws AlluxioException, IOException {
        mFileSystemMaster.createFile(new AlluxioURI(path),
            GrpcUtils.fromProto(mOptionsService, options));
        return CreateFilePResponse.newBuilder().build();
      }

      @Override
      public String toString() {
        return String.format("CreateFile: path=%s, options=%s", path, options);
      }
    }, responseObserver);
  }

  @Override
  public void free(FreePRequest request, StreamObserver<FreePResponse> responseObserver) {
    String path = request.getPath();
    FreePOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, new RpcUtilsNew.RpcCallableThrowsIOException<FreePResponse>() {
      @Override
      public FreePResponse call() throws AlluxioException, IOException {
        mFileSystemMaster.free(new AlluxioURI(path), GrpcUtils.fromProto(mOptionsService, options));
        return FreePResponse.newBuilder().build();
      }

      @Override
      public String toString() {
        return String.format("Free: path=%s, options=%s", path, options);
      }
    }, responseObserver);
  }

  @Override
  public void getNewBlockIdForFile(GetNewBlockIdForFilePRequest request,
      StreamObserver<GetNewBlockIdForFilePResponse> responseObserver) {
    String path = request.getPath();
    GetNewBlockIdForFilePOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, new RpcUtilsNew.RpcCallableThrowsIOException<GetNewBlockIdForFilePResponse>() {
      @Override
      public GetNewBlockIdForFilePResponse call() throws AlluxioException, IOException {
        return GetNewBlockIdForFilePResponse.newBuilder()
            .setId(mFileSystemMaster.getNewBlockIdForFile(new AlluxioURI(path))).build();
      }

      @Override
      public String toString() {
        return String.format("GetNewBlockIdForFile: path=%s, options=%s", path, options);
      }
    }, responseObserver);
  }


  @Override
  public void getStatus(GetStatusPRequest request,
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

  @Override
  public void listStatus(ListStatusPRequest request,
      StreamObserver<ListStatusPResponse> responseObserver) {
    String path = request.getPath();
    ListStatusPOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, new RpcUtilsNew.RpcCallableThrowsIOException<ListStatusPResponse>() {
      @Override
      public ListStatusPResponse call() throws AlluxioException, IOException {
        List<FileInfo> result = new ArrayList<>();
        for (alluxio.wire.FileInfo fileInfo : mFileSystemMaster.listStatus(new AlluxioURI(path),
            GrpcUtils.fromProto(mOptionsService, options))) {
          result.add(GrpcUtils.toProto(fileInfo));
        }
        return ListStatusPResponse.newBuilder().addAllFileInfoList(result).build();
      }

      @Override
      public String toString() {
        return String.format("ListStatus: path=%s, options=%s", path, options);
      }
    }, responseObserver);
  }

  @Override
  public void mount(MountPRequest request, StreamObserver<MountPResponse> responseObserver) {
    String alluxioPath = request.getAlluxioPath();
    String ufsPath = request.getUfsPath();
    MountPOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, new RpcUtilsNew.RpcCallableThrowsIOException<MountPResponse>() {
      @Override
      public MountPResponse call() throws AlluxioException, IOException {
        mFileSystemMaster.mount(new AlluxioURI(alluxioPath), new AlluxioURI(ufsPath),
            GrpcUtils.fromProto(mOptionsService, options));
        return MountPResponse.newBuilder().build();
      }

      @Override
      public String toString() {
        return String.format("Mount: alluxioPath=%s, ufsPath=%s, options=%s", alluxioPath, ufsPath,
            options);
      }
    }, responseObserver);
  }

  @Override
  public void getMountTable(GetMountTablePRequest request,
      StreamObserver<GetMountTablePResponse> responseObserver) {
    RpcUtilsNew.call(LOG, new RpcUtilsNew.RpcCallableThrowsIOException<GetMountTablePResponse>() {
      @Override
      public GetMountTablePResponse call() throws AlluxioException, IOException {
        Map<String, MountPointInfo> mountTableWire = mFileSystemMaster.getMountTable();
        Map<String, alluxio.grpc.MountPointInfo> mountTableProto = new HashMap<>();
        for (Map.Entry<String, MountPointInfo> entry : mountTableWire.entrySet()) {
          mountTableProto.put(entry.getKey(), GrpcUtils.toProto(entry.getValue()));
        }
        return GetMountTablePResponse.newBuilder().putAllMountTable(mountTableProto).build();
      }

      @Override
      public String toString() {
        return String.format("GetMountTable: ");
      }
    }, responseObserver);
  }

  @Override
  public void remove(DeletePRequest request, StreamObserver<DeletePResponse> responseObserver) {
    String path = request.getPath();
    DeletePOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, new RpcUtilsNew.RpcCallableThrowsIOException<DeletePResponse>() {
      @Override
      public DeletePResponse call() throws AlluxioException, IOException {
        mFileSystemMaster.delete(new AlluxioURI(path),
            GrpcUtils.fromProto(mOptionsService, options));
        return DeletePResponse.newBuilder().build();
      }

      @Override
      public String toString() {
        return String.format("Remove: path=%s, options=%s", path, options);
      }
    }, responseObserver);
  }

  @Override
  public void rename(RenamePRequest request, StreamObserver<RenamePResponse> responseObserver) {
    String srcPath = request.getPath();
    String dstPath = request.getDstPath();
    RenamePOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, new RpcUtilsNew.RpcCallableThrowsIOException<RenamePResponse>() {
      @Override
      public RenamePResponse call() throws AlluxioException, IOException {
        mFileSystemMaster.rename(new AlluxioURI(srcPath), new AlluxioURI(dstPath),
            GrpcUtils.fromProto(mOptionsService, options));
        return RenamePResponse.newBuilder().build();
      }

      @Override
      public String toString() {
        return String.format("Rename: srcPath=%s, dstPath=%s, options=%s", srcPath, dstPath,
            options);
      }
    }, responseObserver);
  }

  @Override
  public void scheduleAsyncPersistence(ScheduleAsyncPersistencePRequest request, StreamObserver<ScheduleAsyncPersistencePResponse> responseObserver) {
    String path = request.getPath();
    ScheduleAsyncPersistencePOptions options = request.getOptions();
    RpcUtilsNew.call(LOG,
        new RpcUtilsNew.RpcCallableThrowsIOException<ScheduleAsyncPersistencePResponse>() {
          @Override
          public ScheduleAsyncPersistencePResponse call() throws AlluxioException, IOException {
            mFileSystemMaster.scheduleAsyncPersistence(new AlluxioURI(path));
            return ScheduleAsyncPersistencePResponse.newBuilder().build();
          }

          @Override
          public String toString() {
            return String.format("ScheduleAsyncPersist: path=%s, options=%s", path, options);
          }
        }, responseObserver);
  }

  @Override
  public void setAttribute(SetAttributePRequest request,
      StreamObserver<SetAttributePResponse> responseObserver) {
    String path = request.getPath();
    SetAttributePOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, new RpcUtilsNew.RpcCallableThrowsIOException<SetAttributePResponse>() {
      @Override
      public SetAttributePResponse call() throws AlluxioException, IOException {
        mFileSystemMaster.setAttribute(new AlluxioURI(path),
            GrpcUtils.fromProto(mOptionsService, options));
        return SetAttributePResponse.newBuilder().build();
      }

      @Override
      public String toString() {
        return String.format("SetAttribute: path=%s, options=%s", path, options);
      }
    }, responseObserver);
  }

  @Override
  public void unmount(UnmountPRequest request, StreamObserver<UnmountPResponse> responseObserver) {
    String alluxioPath = request.getAlluxioPath();
    UnmountPOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, new RpcUtilsNew.RpcCallableThrowsIOException<UnmountPResponse>() {
      @Override
      public UnmountPResponse call() throws AlluxioException, IOException {
        mFileSystemMaster.unmount(new AlluxioURI(alluxioPath));
        return UnmountPResponse.newBuilder().build();
      }

      @Override
      public String toString() {
        return String.format("Unmount: alluxioPath=%s, options=%s", alluxioPath, options);
      }
    }, responseObserver);
  }

  @Override
  public void updateUfsMode(UpdateUfsModePRequest request,
      StreamObserver<UpdateUfsModePResponse> responseObserver) {
    String ufsPath = request.getUfsPath();
    UpdateUfsModePOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, new RpcUtilsNew.RpcCallableThrowsIOException<UpdateUfsModePResponse>() {
      @Override
      public UpdateUfsModePResponse call() throws AlluxioException, IOException {
        UfsMode ufsMode;
        switch (options.getUfsMode()) {
          case NoAccess:
            ufsMode = UfsMode.NO_ACCESS;
            break;
          case ReadOnly:
            ufsMode = UfsMode.READ_ONLY;
            break;
          default:
            ufsMode = UfsMode.READ_WRITE;
            break;
        }
        mFileSystemMaster.updateUfsMode(new AlluxioURI(ufsPath), ufsMode);
        return UpdateUfsModePResponse.newBuilder().build();
      }

      @Override
      public String toString() {
        return String.format("UpdateUfsMode: ufsPath=%s, options=%s", ufsPath, options);
      }
    }, responseObserver);
  }
}
