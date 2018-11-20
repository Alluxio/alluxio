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
import alluxio.grpc.FileSystemMasterClientServiceGrpc;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.FreePRequest;
import alluxio.grpc.FreePResponse;
import alluxio.grpc.GetMountTablePRequest;
import alluxio.grpc.GetMountTablePResponse;
import alluxio.grpc.GetNewBlockIdForFilePOptions;
import alluxio.grpc.GetNewBlockIdForFilePRequest;
import alluxio.grpc.GetNewBlockIdForFilePResponse;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.GetStatusPResponse;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.ListStatusPRequest;
import alluxio.grpc.ListStatusPResponse;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.MountPRequest;
import alluxio.grpc.MountPResponse;
import alluxio.grpc.PAclEntry;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.RenamePRequest;
import alluxio.grpc.RenamePResponse;
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.grpc.ScheduleAsyncPersistencePRequest;
import alluxio.grpc.ScheduleAsyncPersistencePResponse;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAclPRequest;
import alluxio.grpc.SetAclPResponse;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.SetAttributePRequest;
import alluxio.grpc.SetAttributePResponse;
import alluxio.grpc.UnmountPOptions;
import alluxio.grpc.UnmountPRequest;
import alluxio.grpc.UnmountPResponse;
import alluxio.grpc.UpdateUfsModePOptions;
import alluxio.grpc.UpdateUfsModePRequest;
import alluxio.grpc.UpdateUfsModePResponse;
import alluxio.master.file.options.CheckConsistencyContext;
import alluxio.master.file.options.CompleteFileContext;
import alluxio.master.file.options.CreateDirectoryContext;
import alluxio.master.file.options.CreateFileContext;
import alluxio.master.file.options.DeleteContext;
import alluxio.master.file.options.FreeContext;
import alluxio.master.file.options.GetStatusContext;
import alluxio.master.file.options.ListStatusContext;
import alluxio.master.file.options.MountContext;
import alluxio.master.file.options.RenameContext;
import alluxio.master.file.options.SetAclContext;
import alluxio.master.file.options.SetAttributeContext;
import alluxio.proto.shared.Acl;
import alluxio.underfs.UfsMode;
import alluxio.util.RpcUtilsNew;
import alluxio.util.grpc.GrpcUtils;
import alluxio.wire.MountPointInfo;

import alluxio.wire.SetAclAction;
import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 * This class is a gRPC handler for file system master RPCs invoked by an Alluxio client.
 */
public final class FileSystemMasterClientServiceHandler
    extends FileSystemMasterClientServiceGrpc.FileSystemMasterClientServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemMasterClientServiceHandler.class);
  private final FileSystemMaster mFileSystemMaster;

  /**
   * Creates a new instance of {@link FileSystemMasterClientServiceHandler}.
   *
   * @param fileSystemMaster the {@link FileSystemMaster} the handler uses internally
   */
  public FileSystemMasterClientServiceHandler(FileSystemMaster fileSystemMaster) {
    Preconditions.checkNotNull(fileSystemMaster, "fileSystemMaster");
    mFileSystemMaster = fileSystemMaster;
  }

  @Override
  public void checkConsistency(CheckConsistencyPRequest request,
      StreamObserver<CheckConsistencyPResponse> responseObserver) {
    String path = request.getPath();
    CheckConsistencyPOptions options = request.getOptions();
    RpcUtilsNew.call(LOG,
        (RpcUtilsNew.RpcCallableThrowsIOException<CheckConsistencyPResponse>) () -> {
          List<AlluxioURI> inconsistentUris = mFileSystemMaster.checkConsistency(
              new AlluxioURI(path), CheckConsistencyContext.defaults(options.toBuilder()));
          List<String> uris = new ArrayList<>(inconsistentUris.size());
          for (AlluxioURI uri : inconsistentUris) {
            uris.add(uri.getPath());
          }
          return CheckConsistencyPResponse.newBuilder().addAllInconsistentPaths(uris).build();
        }, "CheckConsistency", "path=%s, options=%s", responseObserver, path, options);
  }

  @Override
  public void completeFile(CompleteFilePRequest request,
      StreamObserver<CompleteFilePResponse> responseObserver) {
    String path = request.getPath();
    CompleteFilePOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<CompleteFilePResponse>) () -> {
      mFileSystemMaster.completeFile(new AlluxioURI(path),
          CompleteFileContext.defaults(options.toBuilder()));
      return CompleteFilePResponse.newBuilder().build();
    }, "CompleteFile", "path=%s, options=%s", responseObserver, path, options);
  }

  @Override
  public void createDirectory(CreateDirectoryPRequest request,
      StreamObserver<CreateDirectoryPResponse> responseObserver) {
    String path = request.getPath();
    CreateDirectoryPOptions options = request.getOptions();
    RpcUtilsNew.call(LOG,
        (RpcUtilsNew.RpcCallableThrowsIOException<CreateDirectoryPResponse>) () -> {
          mFileSystemMaster.createDirectory(new AlluxioURI(path),
              CreateDirectoryContext.defaults(options.toBuilder()));
          return CreateDirectoryPResponse.newBuilder().build();
        }, "CreateDirectory", "path=%s, options=%s", responseObserver, path, options);
  }

  @Override
  public void createFile(CreateFilePRequest request,
      StreamObserver<CreateFilePResponse> responseObserver) {
    String path = request.getPath();
    CreateFilePOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<CreateFilePResponse>) () -> {
      mFileSystemMaster.createFile(new AlluxioURI(path),
          CreateFileContext.defaults(options.toBuilder()));
      return CreateFilePResponse.newBuilder().build();
    }, "CreateFile", "path=%s, options=%s", responseObserver, path, options);
  }

  @Override
  public void free(FreePRequest request, StreamObserver<FreePResponse> responseObserver) {
    String path = request.getPath();
    FreePOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<FreePResponse>) () -> {
      mFileSystemMaster.free(new AlluxioURI(path), FreeContext.defaults(options.toBuilder()));
      return FreePResponse.newBuilder().build();
    }, "Free", "path=%s, options=%s", responseObserver, path, options);
  }

  @Override
  public void getNewBlockIdForFile(GetNewBlockIdForFilePRequest request,
      StreamObserver<GetNewBlockIdForFilePResponse> responseObserver) {
    String path = request.getPath();
    GetNewBlockIdForFilePOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<GetNewBlockIdForFilePResponse>)
        () -> GetNewBlockIdForFilePResponse.newBuilder()
            .setId(mFileSystemMaster.getNewBlockIdForFile(new AlluxioURI(path))).build(),
        "GetNewBlockIdForFile", "path=%s, options=%s", responseObserver, path, options);
  }

  @Override
  public void getStatus(GetStatusPRequest request,
      StreamObserver<GetStatusPResponse> responseObserver) {
    String path = request.getPath();
    GetStatusPOptions options = request.getOptions();
    RpcUtilsNew.call(LOG,
        (RpcUtilsNew.RpcCallableThrowsIOException<GetStatusPResponse>) () -> GetStatusPResponse
            .newBuilder()
            .setFileInfo(GrpcUtils.toProto(mFileSystemMaster.getFileInfo(new AlluxioURI(path),
                GetStatusContext.defaults(options.toBuilder()))))
            .build(),
        "GetStatus", true, "path=%s, options=%s", responseObserver, path, options);
  }

  @Override
  public void listStatus(ListStatusPRequest request,
      StreamObserver<ListStatusPResponse> responseObserver) {
    String path = request.getPath();
    ListStatusPOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<ListStatusPResponse>) () -> {
      List<FileInfo> result = new ArrayList<>();
      for (alluxio.wire.FileInfo fileInfo : mFileSystemMaster.listStatus(new AlluxioURI(path),
          ListStatusContext.defaults(options.toBuilder()))) {
        result.add(GrpcUtils.toProto(fileInfo));
      }
      return ListStatusPResponse.newBuilder().addAllFileInfoList(result).build();
    }, "ListStatus", "path=%s, options=%s", responseObserver, path, options);
  }

  @Override
  public void mount(MountPRequest request, StreamObserver<MountPResponse> responseObserver) {
    String alluxioPath = request.getAlluxioPath();
    String ufsPath = request.getUfsPath();
    MountPOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<MountPResponse>) () -> {
      mFileSystemMaster.mount(new AlluxioURI(alluxioPath), new AlluxioURI(ufsPath),
          MountContext.defaults(options.toBuilder()));
      return MountPResponse.newBuilder().build();
    }, "Mount", "alluxioPath=%s, ufsPath=%s, options=%s", responseObserver, alluxioPath, ufsPath,
        options);
  }

  @Override
  public void getMountTable(GetMountTablePRequest request,
      StreamObserver<GetMountTablePResponse> responseObserver) {
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<GetMountTablePResponse>) () -> {
      Map<String, MountPointInfo> mountTableWire = mFileSystemMaster.getMountTable();
      Map<String, alluxio.grpc.MountPointInfo> mountTableProto = new HashMap<>();
      for (Map.Entry<String, MountPointInfo> entry : mountTableWire.entrySet()) {
        mountTableProto.put(entry.getKey(), GrpcUtils.toProto(entry.getValue()));
      }
      return GetMountTablePResponse.newBuilder().putAllMountTable(mountTableProto).build();
    }, "GetMountTable", "", responseObserver);
  }

  @Override
  public void remove(DeletePRequest request, StreamObserver<DeletePResponse> responseObserver) {
    String path = request.getPath();
    DeletePOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<DeletePResponse>) () -> {
      mFileSystemMaster.delete(new AlluxioURI(path), DeleteContext.defaults(options.toBuilder()));
      return DeletePResponse.newBuilder().build();
    }, "Remove", "path=%s, options=%s", responseObserver, path, options);
  }

  @Override
  public void rename(RenamePRequest request, StreamObserver<RenamePResponse> responseObserver) {
    String srcPath = request.getPath();
    String dstPath = request.getDstPath();
    RenamePOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<RenamePResponse>) () -> {
      mFileSystemMaster.rename(new AlluxioURI(srcPath), new AlluxioURI(dstPath),
          RenameContext.defaults(options.toBuilder()));
      return RenamePResponse.newBuilder().build();
    }, "Rename", "srcPath=%s, dstPath=%s, options=%s", responseObserver, srcPath, dstPath, options);
  }

  @Override
  public void scheduleAsyncPersistence(ScheduleAsyncPersistencePRequest request,
      StreamObserver<ScheduleAsyncPersistencePResponse> responseObserver) {
    String path = request.getPath();
    ScheduleAsyncPersistencePOptions options = request.getOptions();
    RpcUtilsNew.call(LOG,
        (RpcUtilsNew.RpcCallableThrowsIOException<ScheduleAsyncPersistencePResponse>) () -> {
          mFileSystemMaster.scheduleAsyncPersistence(new AlluxioURI(path));
          return ScheduleAsyncPersistencePResponse.newBuilder().build();
        }, "ScheduleAsyncPersist", "path=%s, options=%s", responseObserver, path, options);
  }

  @Override
  public void setAttribute(SetAttributePRequest request,
      StreamObserver<SetAttributePResponse> responseObserver) {
    String path = request.getPath();
    SetAttributePOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<SetAttributePResponse>) () -> {
      mFileSystemMaster.setAttribute(new AlluxioURI(path),
          SetAttributeContext.defaults(options.toBuilder()));
      return SetAttributePResponse.newBuilder().build();
    }, "SetAttribute", "path=%s, options=%s", responseObserver, path, options);
  }

  @Override
  public void unmount(UnmountPRequest request, StreamObserver<UnmountPResponse> responseObserver) {
    String alluxioPath = request.getAlluxioPath();
    UnmountPOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<UnmountPResponse>) () -> {
      mFileSystemMaster.unmount(new AlluxioURI(alluxioPath));
      return UnmountPResponse.newBuilder().build();
    }, "Unmount", "alluxioPath=%s, options=%s", responseObserver, alluxioPath, options);
  }

  @Override
  public void updateUfsMode(UpdateUfsModePRequest request,
      StreamObserver<UpdateUfsModePResponse> responseObserver) {
    String ufsPath = request.getUfsPath();
    UpdateUfsModePOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<UpdateUfsModePResponse>) () -> {
      UfsMode ufsMode;
      switch (options.getUfsMode()) {
        case NO_ACCESS:
          ufsMode = UfsMode.NO_ACCESS;
          break;
        case READ_ONLY:
          ufsMode = UfsMode.READ_ONLY;
          break;
        default:
          ufsMode = UfsMode.READ_WRITE;
          break;
      }
      mFileSystemMaster.updateUfsMode(new AlluxioURI(ufsPath), ufsMode);
      return UpdateUfsModePResponse.newBuilder().build();
    }, "UpdateUfsMode", "ufsPath=%s, options=%s", responseObserver, ufsPath, options);
  }

  @Override
  public void setAcl(SetAclPRequest request,
                     StreamObserver<SetAclPResponse> responseObserver) {
    String alluxioPath = request.getPath();
    SetAclAction aclAction = GrpcUtils.fromProto(request.getAction());
    List<PAclEntry> aclList = request.getEntriesList();
    SetAclPOptions options = request.getOptions();
    RpcUtilsNew.call(LOG, (RpcUtilsNew.RpcCallableThrowsIOException<SetAclPResponse>) () -> {
      mFileSystemMaster.setAcl(new AlluxioURI(alluxioPath), aclAction,
          aclList.stream().map(GrpcUtils::fromProto).collect(Collectors.toList()),
          SetAclContext.defaults(options.toBuilder()));
      return SetAclPResponse.newBuilder().build();
    }, "setAcl", "alluxioPath=%s, setAclAction=%s, aclEntries=%s, options=%s", responseObserver,
        alluxioPath, aclAction.name(), aclList, options);
  }
}