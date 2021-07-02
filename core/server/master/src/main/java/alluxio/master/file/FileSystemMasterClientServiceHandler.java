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
import alluxio.RpcUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.CheckAccessPRequest;
import alluxio.grpc.CheckAccessPResponse;
import alluxio.grpc.CheckConsistencyPOptions;
import alluxio.grpc.CheckConsistencyPRequest;
import alluxio.grpc.CheckConsistencyPResponse;
import alluxio.grpc.CompleteFilePRequest;
import alluxio.grpc.CompleteFilePResponse;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateDirectoryPRequest;
import alluxio.grpc.CreateDirectoryPResponse;
import alluxio.grpc.CreateFilePRequest;
import alluxio.grpc.CreateFilePResponse;
import alluxio.grpc.DeletePRequest;
import alluxio.grpc.DeletePResponse;
import alluxio.grpc.FileSystemMasterClientServiceGrpc;
import alluxio.grpc.FreePRequest;
import alluxio.grpc.FreePResponse;
import alluxio.grpc.GetFilePathPRequest;
import alluxio.grpc.GetFilePathPResponse;
import alluxio.grpc.GetMountTablePRequest;
import alluxio.grpc.GetMountTablePResponse;
import alluxio.grpc.GetNewBlockIdForFilePRequest;
import alluxio.grpc.GetNewBlockIdForFilePResponse;
import alluxio.grpc.GetStateLockHoldersPRequest;
import alluxio.grpc.GetStateLockHoldersPResponse;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.GetStatusPResponse;
import alluxio.grpc.GetSyncPathListPRequest;
import alluxio.grpc.GetSyncPathListPResponse;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.ListStatusPRequest;
import alluxio.grpc.ListStatusPResponse;
import alluxio.grpc.MountPRequest;
import alluxio.grpc.MountPResponse;
import alluxio.grpc.RenamePRequest;
import alluxio.grpc.RenamePResponse;
import alluxio.grpc.ReverseResolvePRequest;
import alluxio.grpc.ReverseResolvePResponse;
import alluxio.grpc.ScheduleAsyncPersistencePRequest;
import alluxio.grpc.ScheduleAsyncPersistencePResponse;
import alluxio.grpc.SetAclPRequest;
import alluxio.grpc.SetAclPResponse;
import alluxio.grpc.SetAttributePRequest;
import alluxio.grpc.SetAttributePResponse;
import alluxio.grpc.StartSyncPRequest;
import alluxio.grpc.StartSyncPResponse;
import alluxio.grpc.StopSyncPRequest;
import alluxio.grpc.StopSyncPResponse;
import alluxio.grpc.UnmountPRequest;
import alluxio.grpc.UnmountPResponse;
import alluxio.grpc.UpdateMountPRequest;
import alluxio.grpc.UpdateMountPResponse;
import alluxio.grpc.UpdateUfsModePRequest;
import alluxio.grpc.UpdateUfsModePResponse;
import alluxio.master.file.contexts.CheckAccessContext;
import alluxio.master.file.contexts.CheckConsistencyContext;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.FreeContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.GrpcCallTracker;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.contexts.RenameContext;
import alluxio.master.file.contexts.ScheduleAsyncPersistenceContext;
import alluxio.master.file.contexts.SetAclContext;
import alluxio.master.file.contexts.SetAttributeContext;
import alluxio.underfs.UfsMode;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SyncPointInfo;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
  public void checkAccess(CheckAccessPRequest request,
      StreamObserver<CheckAccessPResponse> responseObserver) {
    RpcUtils.call(LOG,
        () -> {
          AlluxioURI pathUri = getAlluxioURI(request.getPath());
          mFileSystemMaster.checkAccess(pathUri,
              CheckAccessContext.create(request.getOptions().toBuilder()));
          return CheckAccessPResponse.getDefaultInstance();
        }, "CheckAccess", "request=%s", responseObserver, request);
  }

  @Override
  public void checkConsistency(CheckConsistencyPRequest request,
      StreamObserver<CheckConsistencyPResponse> responseObserver) {
    CheckConsistencyPOptions options = request.getOptions();
    RpcUtils.call(LOG, () -> {
      AlluxioURI pathUri = getAlluxioURI(request.getPath());
      List<AlluxioURI> inconsistentUris = mFileSystemMaster.checkConsistency(pathUri,
          CheckConsistencyContext.create(options.toBuilder()));
      List<String> uris = new ArrayList<>(inconsistentUris.size());
      for (AlluxioURI uri : inconsistentUris) {
        uris.add(uri.getPath());
      }
      return CheckConsistencyPResponse.newBuilder().addAllInconsistentPaths(uris).build();
    }, "CheckConsistency", "request=%s", responseObserver, request);
  }

  @Override
  public void completeFile(CompleteFilePRequest request,
      StreamObserver<CompleteFilePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      AlluxioURI pathUri = getAlluxioURI(request.getPath());
      mFileSystemMaster.completeFile(pathUri,
          CompleteFileContext.create(request.getOptions().toBuilder()));
      return CompleteFilePResponse.newBuilder().build();
    }, "CompleteFile", "request=%s", responseObserver, request);
  }

  @Override
  public void createDirectory(CreateDirectoryPRequest request,
      StreamObserver<CreateDirectoryPResponse> responseObserver) {
    CreateDirectoryPOptions options = request.getOptions();
    RpcUtils.call(LOG, () -> {
      AlluxioURI pathUri = getAlluxioURI(request.getPath());
      mFileSystemMaster.createDirectory(pathUri, CreateDirectoryContext.create(options.toBuilder())
          .withTracker(new GrpcCallTracker(responseObserver)));
      return CreateDirectoryPResponse.newBuilder().build();
    }, "CreateDirectory", "request=%s", responseObserver, request);
  }

  @Override
  public void createFile(CreateFilePRequest request,
      StreamObserver<CreateFilePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      AlluxioURI pathUri = getAlluxioURI(request.getPath());
      return CreateFilePResponse.newBuilder()
          .setFileInfo(GrpcUtils.toProto(mFileSystemMaster.createFile(pathUri,
              CreateFileContext.create(request.getOptions().toBuilder())
                  .withTracker(new GrpcCallTracker(responseObserver)))))
          .build();
    }, "CreateFile", "request=%s", responseObserver, request);
  }

  @Override
  public void free(FreePRequest request, StreamObserver<FreePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      AlluxioURI pathUri = getAlluxioURI(request.getPath());
      mFileSystemMaster.free(pathUri, FreeContext.create(request.getOptions().toBuilder()));
      return FreePResponse.newBuilder().build();
    }, "Free", "request=%s", responseObserver, request);
  }

  @Override
  public void getNewBlockIdForFile(GetNewBlockIdForFilePRequest request,
      StreamObserver<GetNewBlockIdForFilePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      AlluxioURI pathUri = getAlluxioURI(request.getPath());
      return GetNewBlockIdForFilePResponse.newBuilder()
          .setId(mFileSystemMaster.getNewBlockIdForFile(pathUri)).build();
    }, "GetNewBlockIdForFile", "request=%s", responseObserver, request);
  }

  @Override
  public void getFilePath(GetFilePathPRequest request,
      StreamObserver<GetFilePathPResponse> responseObserver) {
    long fileId = request.getFileId();
    RpcUtils.call(LOG,
        () -> GetFilePathPResponse.newBuilder()
            .setPath(mFileSystemMaster.getPath(fileId).toString()).build(),
        "GetFilePath", true, "request=%s", responseObserver, request);
  }

  @Override
  public void getStatus(GetStatusPRequest request,
      StreamObserver<GetStatusPResponse> responseObserver) {
    String path = request.getPath();
    GetStatusPOptions options = request.getOptions();
    RpcUtils.call(LOG, () -> {
      AlluxioURI pathUri = getAlluxioURI(request.getPath());
      return GetStatusPResponse.newBuilder()
          .setFileInfo(GrpcUtils.toProto(mFileSystemMaster.getFileInfo(pathUri, GetStatusContext
              .create(options.toBuilder()).withTracker(new GrpcCallTracker(responseObserver)))))
          .build();
    }, "GetStatus", true, "request=%s", responseObserver, request);
  }

  @Override
  public void listStatus(ListStatusPRequest request,
      StreamObserver<ListStatusPResponse> responseObserver) {
    final int listStatusBatchSize =
        ServerConfiguration.getInt(PropertyKey.MASTER_FILE_SYSTEM_LISTSTATUS_RESULTS_PER_MESSAGE);

    // Result streamer for listStatus.
    ListStatusResultStream resultStream =
        new ListStatusResultStream(listStatusBatchSize, responseObserver);

    try {
      RpcUtils.callAndReturn(LOG, () -> {
        AlluxioURI pathUri = getAlluxioURI(request.getPath());
        mFileSystemMaster.listStatus(pathUri,
            ListStatusContext.create(request.getOptions().toBuilder())
                .withTracker(new GrpcCallTracker(responseObserver)),
            resultStream);
        // Return just something.
        return null;
      }, "ListStatus", false, "request=%s", request);
    } catch (Exception e) {
      resultStream.fail(e);
    } finally {
      resultStream.complete();
    }
  }

  @Override
  public void mount(MountPRequest request, StreamObserver<MountPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mFileSystemMaster.mount(new AlluxioURI(request.getAlluxioPath()),
          new AlluxioURI(request.getUfsPath()),
          MountContext.create(request.getOptions().toBuilder())
              .withTracker(new GrpcCallTracker(responseObserver)));
      return MountPResponse.newBuilder().build();
    }, "Mount", "request=%s", responseObserver, request);
  }

  @Override
  public void updateMount(UpdateMountPRequest request,
      StreamObserver<UpdateMountPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mFileSystemMaster.updateMount(new AlluxioURI(request.getAlluxioPath()),
          MountContext.create(request.getOptions().toBuilder())
              .withTracker(new GrpcCallTracker(responseObserver)));
      return UpdateMountPResponse.newBuilder().build();
    }, "UpdateMount", "request=%s", responseObserver, request);
  }

  @Override
  public void getMountTable(GetMountTablePRequest request,
      StreamObserver<GetMountTablePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      Map<String, MountPointInfo> mountTableWire = mFileSystemMaster.getMountPointInfoSummary();
      Map<String, alluxio.grpc.MountPointInfo> mountTableProto = new HashMap<>();
      for (Map.Entry<String, MountPointInfo> entry : mountTableWire.entrySet()) {
        mountTableProto.put(entry.getKey(), GrpcUtils.toProto(entry.getValue()));
      }
      return GetMountTablePResponse.newBuilder().putAllMountPoints(mountTableProto).build();
    }, "GetMountTable", "request=%s", responseObserver, request);
  }

  @Override
  public void getSyncPathList(GetSyncPathListPRequest request,
      StreamObserver<GetSyncPathListPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      List<SyncPointInfo> pathList = mFileSystemMaster.getSyncPathList();
      List<alluxio.grpc.SyncPointInfo> syncPointInfoList =
          pathList.stream().map(SyncPointInfo::toProto).collect(Collectors.toList());
      return GetSyncPathListPResponse.newBuilder().addAllSyncPaths(syncPointInfoList).build();
    }, "getSyncPathList", "request=%s", responseObserver, request);
  }

  @Override
  public void remove(DeletePRequest request, StreamObserver<DeletePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      AlluxioURI pathUri = getAlluxioURI(request.getPath());
      mFileSystemMaster.delete(pathUri, DeleteContext.create(request.getOptions().toBuilder())
          .withTracker(new GrpcCallTracker(responseObserver)));
      return DeletePResponse.newBuilder().build();
    }, "Remove", "request=%s", responseObserver, request);
  }

  @Override
  public void rename(RenamePRequest request, StreamObserver<RenamePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      AlluxioURI srcPathUri = getAlluxioURI(request.getPath());
      AlluxioURI dstPathUri = getAlluxioURI(request.getDstPath());
      mFileSystemMaster.rename(srcPathUri, dstPathUri,
          RenameContext.create(request.getOptions().toBuilder())
              .withTracker(new GrpcCallTracker(responseObserver)));
      return RenamePResponse.newBuilder().build();
    }, "Rename", "request=%s", responseObserver, request);
  }

  @Override
  public void reverseResolve(ReverseResolvePRequest request,
      StreamObserver<ReverseResolvePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      AlluxioURI ufsUri = new AlluxioURI(request.getUfsUri());
      AlluxioURI alluxioPath = mFileSystemMaster.reverseResolve(ufsUri);
      return ReverseResolvePResponse.newBuilder().setAlluxioPath(alluxioPath.getPath()).build();
    }, "ReverseResolve", "request=%s", responseObserver, request);
  }

  @Override
  public void scheduleAsyncPersistence(ScheduleAsyncPersistencePRequest request,
      StreamObserver<ScheduleAsyncPersistencePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mFileSystemMaster.scheduleAsyncPersistence(new AlluxioURI(request.getPath()),
          ScheduleAsyncPersistenceContext.create(request.getOptions().toBuilder()));
      return ScheduleAsyncPersistencePResponse.newBuilder().build();
    }, "ScheduleAsyncPersist", "request=%s", responseObserver, request);
  }

  @Override
  public void setAttribute(SetAttributePRequest request,
      StreamObserver<SetAttributePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      AlluxioURI pathUri = getAlluxioURI(request.getPath());
      mFileSystemMaster.setAttribute(pathUri,
          SetAttributeContext.create(request.getOptions().toBuilder())
              .withTracker(new GrpcCallTracker(responseObserver)));
      return SetAttributePResponse.newBuilder().build();
    }, "SetAttribute", "request=%s", responseObserver, request);
  }

  @Override
  public void startSync(StartSyncPRequest request,
      StreamObserver<StartSyncPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mFileSystemMaster.startSync(new AlluxioURI(request.getPath()));
      return StartSyncPResponse.newBuilder().build();
    }, "startSync", "request=%s", responseObserver, request);
  }

  @Override
  public void stopSync(StopSyncPRequest request,
      StreamObserver<StopSyncPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mFileSystemMaster.stopSync(new AlluxioURI(request.getPath()));
      return StopSyncPResponse.newBuilder().build();
    }, "stopSync", "request=%s", responseObserver, request);
  }

  @Override
  public void unmount(UnmountPRequest request, StreamObserver<UnmountPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mFileSystemMaster.unmount(new AlluxioURI(request.getAlluxioPath()));
      return UnmountPResponse.newBuilder().build();
    }, "Unmount", "request=%s", responseObserver, request);
  }

  @Override
  public void updateUfsMode(UpdateUfsModePRequest request,
      StreamObserver<UpdateUfsModePResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      UfsMode ufsMode;
      switch (request.getOptions().getUfsMode()) {
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
      mFileSystemMaster.updateUfsMode(new AlluxioURI(request.getUfsPath()), ufsMode);
      return UpdateUfsModePResponse.newBuilder().build();
    }, "UpdateUfsMode", "request=%s", responseObserver, request);
  }

  @Override
  public void setAcl(SetAclPRequest request, StreamObserver<SetAclPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      AlluxioURI pathUri = getAlluxioURI(request.getPath());
      mFileSystemMaster.setAcl(pathUri, request.getAction(),
          request.getEntriesList().stream().map(GrpcUtils::fromProto).collect(Collectors.toList()),
          SetAclContext.create(request.getOptions().toBuilder())
              .withTracker(new GrpcCallTracker(responseObserver)));
      return SetAclPResponse.newBuilder().build();
    }, "setAcl", "request=%s", responseObserver, request);
  }

  @Override
  public void getStateLockHolders(GetStateLockHoldersPRequest request,
                                  StreamObserver<GetStateLockHoldersPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      final List<String> holders = mFileSystemMaster.getStateLockSharedWaitersAndHolders();
      return GetStateLockHoldersPResponse.newBuilder().addAllThreads(holders).build();
    }, "getStateLockHolders", "request=%s", responseObserver, request);
  }

  /**
   * Helper to return {@link AlluxioURI} from transport URI.
   *
   * @param uriStr transport uri string
   * @return a {@link AlluxioURI} instance
   */
  private AlluxioURI getAlluxioURI(String uriStr) throws InvalidPathException {
    return new AlluxioURI(uriStr);
  }
}
