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

import alluxio.dora.AlluxioURI;
import alluxio.dora.RpcUtils;
import alluxio.dora.conf.Configuration;
import alluxio.dora.conf.PropertyKey;
import alluxio.dora.exception.AlluxioException;
import alluxio.dora.master.scheduler.Scheduler;
import alluxio.dora.grpc.CheckAccessPRequest;
import alluxio.dora.grpc.CheckAccessPResponse;
import alluxio.dora.grpc.CheckConsistencyPOptions;
import alluxio.dora.grpc.CheckConsistencyPRequest;
import alluxio.dora.grpc.CheckConsistencyPResponse;
import alluxio.dora.grpc.CompleteFilePRequest;
import alluxio.dora.grpc.CompleteFilePResponse;
import alluxio.dora.grpc.CreateDirectoryPOptions;
import alluxio.dora.grpc.CreateDirectoryPRequest;
import alluxio.dora.grpc.CreateDirectoryPResponse;
import alluxio.dora.grpc.CreateFilePRequest;
import alluxio.dora.grpc.CreateFilePResponse;
import alluxio.dora.grpc.DeletePRequest;
import alluxio.dora.grpc.DeletePResponse;
import alluxio.dora.grpc.ExistsPRequest;
import alluxio.dora.grpc.ExistsPResponse;
import alluxio.dora.grpc.FileSystemMasterClientServiceGrpc;
import alluxio.dora.grpc.FreePRequest;
import alluxio.dora.grpc.FreePResponse;
import alluxio.dora.grpc.GetFilePathPRequest;
import alluxio.dora.grpc.GetFilePathPResponse;
import alluxio.dora.grpc.GetJobProgressPRequest;
import alluxio.dora.grpc.GetJobProgressPResponse;
import alluxio.dora.grpc.GetMountTablePRequest;
import alluxio.dora.grpc.GetMountTablePResponse;
import alluxio.dora.grpc.GetNewBlockIdForFilePRequest;
import alluxio.dora.grpc.GetNewBlockIdForFilePResponse;
import alluxio.dora.grpc.GetStateLockHoldersPRequest;
import alluxio.dora.grpc.GetStateLockHoldersPResponse;
import alluxio.dora.grpc.GetStatusPOptions;
import alluxio.dora.grpc.GetStatusPRequest;
import alluxio.dora.grpc.GetStatusPResponse;
import alluxio.dora.grpc.GetSyncPathListPRequest;
import alluxio.dora.grpc.GetSyncPathListPResponse;
import alluxio.dora.grpc.GrpcUtils;
import alluxio.dora.grpc.JobProgressReportFormat;
import alluxio.dora.grpc.ListStatusPRequest;
import alluxio.dora.grpc.ListStatusPResponse;
import alluxio.dora.grpc.ListStatusPartialPRequest;
import alluxio.dora.grpc.ListStatusPartialPResponse;
import alluxio.dora.grpc.MountPRequest;
import alluxio.dora.grpc.MountPResponse;
import alluxio.dora.grpc.NeedsSyncRequest;
import alluxio.dora.grpc.NeedsSyncResponse;
import alluxio.dora.grpc.RenamePRequest;
import alluxio.dora.grpc.RenamePResponse;
import alluxio.dora.grpc.ReverseResolvePRequest;
import alluxio.dora.grpc.ReverseResolvePResponse;
import alluxio.dora.grpc.ScheduleAsyncPersistencePRequest;
import alluxio.dora.grpc.ScheduleAsyncPersistencePResponse;
import alluxio.dora.grpc.SetAclPRequest;
import alluxio.dora.grpc.SetAclPResponse;
import alluxio.dora.grpc.SetAttributePRequest;
import alluxio.dora.grpc.SetAttributePResponse;
import alluxio.dora.grpc.StartSyncPRequest;
import alluxio.dora.grpc.StartSyncPResponse;
import alluxio.dora.grpc.StopJobPRequest;
import alluxio.dora.grpc.StopJobPResponse;
import alluxio.dora.grpc.StopSyncPRequest;
import alluxio.dora.grpc.StopSyncPResponse;
import alluxio.dora.grpc.SubmitJobPRequest;
import alluxio.dora.grpc.SubmitJobPResponse;
import alluxio.dora.grpc.UnmountPRequest;
import alluxio.dora.grpc.UnmountPResponse;
import alluxio.dora.grpc.UpdateMountPRequest;
import alluxio.dora.grpc.UpdateMountPResponse;
import alluxio.dora.grpc.UpdateUfsModePRequest;
import alluxio.dora.grpc.UpdateUfsModePResponse;
import alluxio.dora.job.JobDescription;
import alluxio.dora.job.JobRequest;
import alluxio.job.util.SerializationUtils;
import alluxio.dora.master.file.contexts.CheckAccessContext;
import alluxio.dora.master.file.contexts.CheckConsistencyContext;
import alluxio.dora.master.file.contexts.CompleteFileContext;
import alluxio.dora.master.file.contexts.CreateDirectoryContext;
import alluxio.dora.master.file.contexts.CreateFileContext;
import alluxio.dora.master.file.contexts.DeleteContext;
import alluxio.dora.master.file.contexts.ExistsContext;
import alluxio.dora.master.file.contexts.FreeContext;
import alluxio.dora.master.file.contexts.GetStatusContext;
import alluxio.dora.master.file.contexts.GrpcCallTracker;
import alluxio.dora.master.file.contexts.ListStatusContext;
import alluxio.dora.master.file.contexts.MountContext;
import alluxio.dora.master.file.contexts.RenameContext;
import alluxio.dora.master.file.contexts.ScheduleAsyncPersistenceContext;
import alluxio.dora.master.file.contexts.SetAclContext;
import alluxio.dora.master.file.contexts.SetAttributeContext;
import alluxio.dora.master.job.JobFactoryProducer;
import alluxio.dora.recorder.Recorder;
import alluxio.scheduler.job.Job;
import alluxio.dora.underfs.UfsMode;
import alluxio.dora.wire.MountPointInfo;
import alluxio.dora.wire.SyncPointInfo;

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
  private final Scheduler mScheduler;

  /**
   * Creates a new instance of {@link FileSystemMasterClientServiceHandler}.
   *
   * @param fileSystemMaster the {@link FileSystemMaster} the handler uses internally
   * @param scheduler the {@link Scheduler}
   */
  public FileSystemMasterClientServiceHandler(FileSystemMaster fileSystemMaster,
      Scheduler scheduler) {
    Preconditions.checkNotNull(fileSystemMaster, "fileSystemMaster");
    mFileSystemMaster = fileSystemMaster;
    mScheduler = Preconditions.checkNotNull(scheduler, "scheduler");
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
  public void exists(ExistsPRequest request,
          StreamObserver<ExistsPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      AlluxioURI pathUri = getAlluxioURI(request.getPath());
      boolean exists = mFileSystemMaster.exists(pathUri,
          ExistsContext.create(request.getOptions().toBuilder()));
      return ExistsPResponse.newBuilder().setExists(exists).build();
    }, "CheckExistence", "request=%s", responseObserver, request);
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
        Configuration.getInt(PropertyKey.MASTER_FILE_SYSTEM_LISTSTATUS_RESULTS_PER_MESSAGE);

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
  public void listStatusPartial(ListStatusPartialPRequest request,
                                StreamObserver<ListStatusPartialPResponse> responseObserver) {
    ListStatusContext context = ListStatusContext.create(request.getOptions().toBuilder());
    ListStatusPartialResultStream resultStream =
        new ListStatusPartialResultStream(responseObserver, context);
    try {
      RpcUtils.callAndReturn(LOG, () -> {
        AlluxioURI pathUri = getAlluxioURI(request.getPath());
        mFileSystemMaster.listStatus(pathUri,
            context.withTracker(new GrpcCallTracker(responseObserver)),
            resultStream);
        return null;
      }, "ListStatus", false, "request=%s", request);
    } catch (Exception e) {
      resultStream.onError(e);
    } finally {
      resultStream.complete();
    }
  }

  @Override
  public void mount(MountPRequest request, StreamObserver<MountPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      MountContext mountContext = MountContext.create(request.getOptions().toBuilder())
          .withTracker(new GrpcCallTracker(responseObserver));
      // the mount execution process is recorded so that
      // when an exception occurs during mounting, the user can get detailed debugging messages
      try {
        mFileSystemMaster.mount(new AlluxioURI(request.getAlluxioPath()),
            new AlluxioURI(request.getUfsPath()), mountContext);
      } catch (Exception e) {
        Recorder recorder = mountContext.getRecorder();
        recorder.record(e.getMessage());
        // put the messages in an exception and let it carry over to the user
        throw new AlluxioException(String.join("\n", recorder.takeRecords()), e);
      }
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
      // Set the checkUfs default to true to include ufs usage info, etc.,
      // which requires talking to UFS and comes at a cost.
      boolean checkUfs = request.hasCheckUfs() ? request.getCheckUfs() : true;
      Map<String, MountPointInfo> mountTableWire = mFileSystemMaster.getMountPointInfoSummary(
          checkUfs);
      Map<String, alluxio.dora.grpc.MountPointInfo> mountTableProto = new HashMap<>();
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
      List<alluxio.dora.grpc.SyncPointInfo> syncPointInfoList =
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

  @Override
  public void needsSync(NeedsSyncRequest request,
                        StreamObserver<NeedsSyncResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mFileSystemMaster.needsSync(new AlluxioURI(request.getPath()));
      return NeedsSyncResponse.getDefaultInstance();
    }, "NeedsSync", true, "request=%s", responseObserver, request);
  }

  @Override
  public void submitJob(SubmitJobPRequest request,
      StreamObserver<SubmitJobPResponse> responseObserver) {

    RpcUtils.call(LOG, () -> {
      JobRequest jobRequest;
      try {
        jobRequest = (JobRequest) SerializationUtils.deserialize(request
            .getRequestBody()
            .toByteArray());
      } catch (Exception e) {
        throw new IllegalArgumentException("fail to parse job request", e);
      }
      Job<?> job = JobFactoryProducer.create(jobRequest, mFileSystemMaster).create();
      boolean submitted = mScheduler.submitJob(job);
      SubmitJobPResponse.Builder builder = SubmitJobPResponse.newBuilder();
      if (submitted) {
        builder.setJobId(job.getJobId());
      }
      return builder.build();
    }, "submitJob", "request=%s", responseObserver, request);
  }

  @Override
  public void stopJob(StopJobPRequest request,
      StreamObserver<StopJobPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      boolean stopped = mScheduler.stopJob(JobDescription.from(request.getJobDescription()));
      return alluxio.dora.grpc.StopJobPResponse.newBuilder()
          .setJobStopped(stopped)
          .build();
    }, "stopJob", "request=%s", responseObserver, request);
  }

  @Override
  public void getJobProgress(GetJobProgressPRequest request,
      StreamObserver<GetJobProgressPResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      JobProgressReportFormat format = JobProgressReportFormat.TEXT;
      if (request.hasOptions() && request.getOptions().hasFormat()) {
        format = request.getOptions().getFormat();
      }
      boolean verbose = false;
      if (request.hasOptions() && request.getOptions().hasVerbose()) {
        verbose = request.getOptions().getVerbose();
      }
      return GetJobProgressPResponse.newBuilder()
          .setProgressReport(mScheduler.getJobProgress(
              JobDescription.from(request.getJobDescription()), format, verbose))
          .build();
    }, "getJobProgress", "request=%s", responseObserver, request);
  }

  /**
   * Helper to return {@link AlluxioURI} from transport URI.
   *
   * @param uriStr transport uri string
   * @return a {@link AlluxioURI} instance
   */
  private AlluxioURI getAlluxioURI(String uriStr) {
    return new AlluxioURI(uriStr);
  }
}
