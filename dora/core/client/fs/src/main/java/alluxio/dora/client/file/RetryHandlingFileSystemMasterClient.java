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

package alluxio.dora.client.file;

import alluxio.dora.AbstractMasterClient;
import alluxio.dora.AlluxioURI;
import alluxio.dora.Constants;
import alluxio.dora.exception.status.AlluxioStatusException;
import alluxio.dora.wire.MountPointInfo;
import alluxio.dora.grpc.CheckAccessPOptions;
import alluxio.dora.grpc.CheckAccessPRequest;
import alluxio.dora.grpc.CheckConsistencyPOptions;
import alluxio.dora.grpc.CheckConsistencyPRequest;
import alluxio.dora.grpc.CompleteFilePOptions;
import alluxio.dora.grpc.CompleteFilePRequest;
import alluxio.dora.grpc.CreateDirectoryPOptions;
import alluxio.dora.grpc.CreateDirectoryPRequest;
import alluxio.dora.grpc.CreateFilePOptions;
import alluxio.dora.grpc.CreateFilePRequest;
import alluxio.dora.grpc.DeletePOptions;
import alluxio.dora.grpc.DeletePRequest;
import alluxio.dora.grpc.ExistsPOptions;
import alluxio.dora.grpc.ExistsPRequest;
import alluxio.dora.grpc.FileSystemMasterClientServiceGrpc;
import alluxio.dora.grpc.FreePOptions;
import alluxio.dora.grpc.FreePRequest;
import alluxio.dora.grpc.GetFilePathPRequest;
import alluxio.dora.grpc.GetJobProgressPRequest;
import alluxio.dora.grpc.GetJobProgressPResponse;
import alluxio.dora.grpc.GetMountTablePRequest;
import alluxio.dora.grpc.GetNewBlockIdForFilePOptions;
import alluxio.dora.grpc.GetNewBlockIdForFilePRequest;
import alluxio.dora.grpc.GetStateLockHoldersPOptions;
import alluxio.dora.grpc.GetStateLockHoldersPRequest;
import alluxio.dora.grpc.GetStatusPOptions;
import alluxio.dora.grpc.GetStatusPRequest;
import alluxio.dora.grpc.GetSyncPathListPRequest;
import alluxio.dora.grpc.GrpcUtils;
import alluxio.dora.grpc.JobProgressPOptions;
import alluxio.dora.grpc.JobProgressReportFormat;
import alluxio.dora.grpc.ListStatusPOptions;
import alluxio.dora.grpc.ListStatusPRequest;
import alluxio.dora.grpc.ListStatusPartialPOptions;
import alluxio.dora.grpc.ListStatusPartialPRequest;
import alluxio.dora.grpc.MountPOptions;
import alluxio.dora.grpc.MountPRequest;
import alluxio.dora.grpc.NeedsSyncRequest;
import alluxio.dora.grpc.RenamePOptions;
import alluxio.dora.grpc.RenamePRequest;
import alluxio.dora.grpc.ReverseResolvePRequest;
import alluxio.dora.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.dora.grpc.ScheduleAsyncPersistencePRequest;
import alluxio.dora.grpc.ServiceType;
import alluxio.dora.grpc.SetAclAction;
import alluxio.dora.grpc.SetAclPOptions;
import alluxio.dora.grpc.SetAclPRequest;
import alluxio.dora.grpc.SetAttributePOptions;
import alluxio.dora.grpc.SetAttributePRequest;
import alluxio.dora.grpc.StartSyncPRequest;
import alluxio.dora.grpc.StopJobPRequest;
import alluxio.dora.grpc.StopJobPResponse;
import alluxio.dora.grpc.StopSyncPRequest;
import alluxio.dora.grpc.SubmitJobPRequest;
import alluxio.dora.grpc.SubmitJobPResponse;
import alluxio.dora.grpc.UnmountPOptions;
import alluxio.dora.grpc.UnmountPRequest;
import alluxio.dora.grpc.UpdateMountPRequest;
import alluxio.dora.grpc.UpdateUfsModePOptions;
import alluxio.dora.grpc.UpdateUfsModePRequest;
import alluxio.dora.job.JobDescription;
import alluxio.dora.job.JobRequest;
import alluxio.dora.master.MasterClientContext;
import alluxio.dora.retry.CountingRetry;
import alluxio.dora.security.authorization.AclEntry;
import alluxio.dora.util.FileSystemOptionsUtils;
import alluxio.dora.wire.SyncPointInfo;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the file system master, used by alluxio clients.
 *
 */
@ThreadSafe
public final class RetryHandlingFileSystemMasterClient extends AbstractMasterClient
    implements FileSystemMasterClient {
  private static final Logger RPC_LOG = LoggerFactory.getLogger(FileSystemMasterClient.class);

  private FileSystemMasterClientServiceGrpc.FileSystemMasterClientServiceBlockingStub mClient =
      null;

  /**
   * Creates a new {@link RetryHandlingFileSystemMasterClient} instance.
   *
   * @param conf master client configuration
   */
  public RetryHandlingFileSystemMasterClient(MasterClientContext conf) {
    super(conf);
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.FILE_SYSTEM_MASTER_CLIENT_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = FileSystemMasterClientServiceGrpc.newBlockingStub(mChannel);
  }

  @Override
  public void checkAccess(AlluxioURI path, CheckAccessPOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mClient.checkAccess(
        CheckAccessPRequest.newBuilder().setPath(getTransportPath(path))
            .setOptions(options).build()),
        RPC_LOG, "CheckAccess", "path=%s,options=%s", path, options);
  }

  @Override
  public List<AlluxioURI> checkConsistency(final AlluxioURI path,
      final CheckConsistencyPOptions options) throws AlluxioStatusException {
    return retryRPC(() -> {
      List<String> inconsistentPaths = mClient.checkConsistency(CheckConsistencyPRequest
          .newBuilder().setPath(getTransportPath(path)).setOptions(options).build())
          .getInconsistentPathsList();
      List<AlluxioURI> inconsistentUris = new ArrayList<>(inconsistentPaths.size());
      for (String inconsistentPath : inconsistentPaths) {
        inconsistentUris.add(new AlluxioURI(inconsistentPath));
      }
      return inconsistentUris;
    }, RPC_LOG, "CheckConsistency", "path=%s,options=%s", path, options);
  }

  @Override
  public void createDirectory(final AlluxioURI path,
      final CreateDirectoryPOptions options) throws AlluxioStatusException {
    retryRPC(
        () -> mClient.createDirectory(CreateDirectoryPRequest.newBuilder()
            .setPath(getTransportPath(path)).setOptions(options).build()),
        RPC_LOG, "CreateDirectory", "path=%s,options=%s", path, options);
  }

  @Override
  public URIStatus createFile(final AlluxioURI path, final CreateFilePOptions options)
      throws AlluxioStatusException {
    return retryRPC(
        () -> new URIStatus(GrpcUtils.fromProto(mClient.createFile(CreateFilePRequest.newBuilder()
            .setPath(getTransportPath(path)).setOptions(options).build()).getFileInfo())),
        RPC_LOG, "CreateFile", "path=%s,options=%s", path, options);
  }

  @Override
  public void completeFile(final AlluxioURI path, final CompleteFilePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mClient.completeFile(CompleteFilePRequest.newBuilder()
        .setPath(getTransportPath(path)).setOptions(options).build()), RPC_LOG, "CompleteFile",
        "path=%s,options=%s", path, options);
  }

  @Override
  public void delete(final AlluxioURI path, final DeletePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mClient.remove(DeletePRequest.newBuilder().setPath(getTransportPath(path))
        .setOptions(options).build()), RPC_LOG, "Delete",
        "path=%s,options=%s", path, options);
  }

  @Override
  public boolean exists(final AlluxioURI path, final ExistsPOptions options)
      throws AlluxioStatusException {
    return retryRPC(() -> mClient.exists(ExistsPRequest.newBuilder()
        .setPath(getTransportPath(path))
        .setOptions(options).build()).getExists(), RPC_LOG, "Exists",
        "path=%s,options=%s", path, options);
  }

  @Override
  public void free(final AlluxioURI path, final FreePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mClient.free(FreePRequest.newBuilder().setPath(getTransportPath(path))
        .setOptions(options).build()), RPC_LOG, "Free", "path=%s,options=%s", path, options);
  }

  @Override
  public String getFilePath(long fileId) throws AlluxioStatusException {
    return retryRPC(() -> mClient.getFilePath(GetFilePathPRequest
            .newBuilder().setFileId(fileId).build()).getPath(), RPC_LOG, "GetFilePath", "fileId=%d",
        fileId);
  }

  @Override
  public URIStatus getStatus(final AlluxioURI path, final GetStatusPOptions options)
      throws AlluxioStatusException {
    return retryRPC(() -> new URIStatus(GrpcUtils
        .fromProto(mClient.getStatus(GetStatusPRequest.newBuilder().setPath(getTransportPath(path))
            .setOptions(options).build()).getFileInfo())),
        RPC_LOG, "GetStatus", "path=%s,options=%s", path, options);
  }

  @Override
  public synchronized List<SyncPointInfo> getSyncPathList() throws AlluxioStatusException {
    return retryRPC(() -> mClient.getSyncPathList(GetSyncPathListPRequest.getDefaultInstance())
        .getSyncPathsList().stream().map(x -> SyncPointInfo.fromProto(x))
        .collect(Collectors.toList()), RPC_LOG, "GetSyncPathList", "");
  }

  @Override
  public long getNewBlockIdForFile(final AlluxioURI path)
      throws AlluxioStatusException {
    return retryRPC(
        () -> mClient.getNewBlockIdForFile(
            GetNewBlockIdForFilePRequest.newBuilder().setPath(getTransportPath(path))
                .setOptions(GetNewBlockIdForFilePOptions.newBuilder().build()).build())
            .getId(),
        RPC_LOG, "GetNewBlockIdForFile", "path=%s", path);
  }

  @Override
  public Map<String, MountPointInfo> getMountTable(boolean checkUfs)
      throws AlluxioStatusException {
    return retryRPC(() -> {
      Map<String, MountPointInfo> mountTableWire = new HashMap<>();
      for (Map.Entry<String, alluxio.dora.grpc.MountPointInfo> entry : mClient
          .getMountTable(GetMountTablePRequest.newBuilder().setCheckUfs(checkUfs).build())
          .getMountPointsMap()
          .entrySet()) {
        mountTableWire.put(entry.getKey(), GrpcUtils.fromProto(entry.getValue()));
      }
      return mountTableWire;
    }, RPC_LOG, "GetMountTable", "");
  }

  @Override
  public void iterateStatus(final AlluxioURI path, final ListStatusPOptions options,
      Consumer<? super URIStatus> action)
      throws AlluxioStatusException {
    retryRPC(
         new CountingRetry(0),
        () ->  {
          StreamSupport.stream(
              Spliterators.spliteratorUnknownSize(
                  mClient.listStatus(ListStatusPRequest.newBuilder()
                      .setPath(getTransportPath(path)).setOptions(options).build()),
                  Spliterator.ORDERED),
              false)
              .flatMap(pListStatusResponse -> pListStatusResponse.getFileInfosList().stream()
                  .map(pFileInfo -> new URIStatus(GrpcUtils.fromProto(pFileInfo))))
              .forEach(action);
          return null;
        },
        RPC_LOG, "ListStatus", "path=%s,options=%s", path, options);
  }

  @Override
  public List<URIStatus> listStatus(final AlluxioURI path, final ListStatusPOptions options)
      throws AlluxioStatusException {
    return retryRPC(() -> {
      List<URIStatus> result = new ArrayList<>();
      mClient
          .listStatus(ListStatusPRequest.newBuilder().setPath(getTransportPath(path))
              .setOptions(options).build())
          .forEachRemaining(
              (pListStatusResponse) -> result.addAll(pListStatusResponse.getFileInfosList().stream()
                  .map((pFileInfo) -> new URIStatus(GrpcUtils.fromProto(pFileInfo)))
                  .collect(Collectors.toList())));
      return result;
    }, RPC_LOG, "ListStatus", "path=%s,options=%s", path, options);
  }

  @Override
  public ListStatusPartialResult listStatusPartial(
      final AlluxioURI path, final ListStatusPartialPOptions options)
      throws AlluxioStatusException {
    return retryRPC(() -> ListStatusPartialResult.fromProto(mClient
        .listStatusPartial(ListStatusPartialPRequest.newBuilder().setPath(getTransportPath(path))
            .setOptions(options).build())), RPC_LOG,
        "ListStatusPartial", "path=%s,options=%s", path, options);
  }

  @Override
  public void mount(final AlluxioURI alluxioPath, final AlluxioURI ufsPath,
      final MountPOptions options) throws AlluxioStatusException {
    retryRPC(
        () -> mClient.mount(MountPRequest.newBuilder().setAlluxioPath(alluxioPath.toString())
            .setUfsPath(ufsPath.toString()).setOptions(options).build()),
        RPC_LOG, "Mount", "alluxioPath=%s,ufsPath=%s,options=%s", alluxioPath, ufsPath, options);
  }

  @Override
  public void updateMount(final AlluxioURI alluxioPath, final MountPOptions options)
      throws AlluxioStatusException {
    retryRPC(
        () -> mClient.updateMount(UpdateMountPRequest.newBuilder()
            .setAlluxioPath(alluxioPath.toString())
            .setOptions(options).build()),
        RPC_LOG, "UpdateMount", "path=%s,options=%s", alluxioPath, options);
  }

  @Override
  public void rename(final AlluxioURI src, final AlluxioURI dst)
      throws AlluxioStatusException {
    rename(src, dst, FileSystemOptionsUtils.renameDefaults(mContext.getClusterConf()));
  }

  @Override
  public void rename(final AlluxioURI src, final AlluxioURI dst,
      final RenamePOptions options) throws AlluxioStatusException {
    retryRPC(() -> mClient.rename(RenamePRequest.newBuilder().setPath(getTransportPath(src))
        .setDstPath(getTransportPath(dst)).setOptions(options).build()), RPC_LOG, "Rename",
        "src=%s,dst=%s,options=%s", src, dst, options);
  }

  @Override
  public AlluxioURI reverseResolve(final AlluxioURI ufsUri) throws AlluxioStatusException {
    return retryRPC(() -> new AlluxioURI(mClient.reverseResolve(ReverseResolvePRequest.newBuilder()
        .setUfsUri(ufsUri.toString()).build()).getAlluxioPath()), RPC_LOG, "ReverseResolve",
        "ufsUri=%s", ufsUri);
  }

  @Override
  public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries,
      SetAclPOptions options) throws AlluxioStatusException {
    retryRPC(() -> mClient.setAcl(
        SetAclPRequest.newBuilder().setPath(getTransportPath(path)).setAction(action)
            .addAllEntries(entries.stream().map(GrpcUtils::toProto).collect(Collectors.toList()))
            .setOptions(options).build()),
        RPC_LOG, "SetAcl", "path=%s,action=%s,entries=%s,options=%s",
        path, action, entries, options);
  }

  @Override
  public void setAttribute(final AlluxioURI path, final SetAttributePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mClient.setAttribute(SetAttributePRequest.newBuilder()
        .setPath(getTransportPath(path)).setOptions(options).build()), RPC_LOG, "SetAttribute",
        "path=%s,options=%s", path, options);
  }

  @Override
  public void scheduleAsyncPersist(final AlluxioURI path, ScheduleAsyncPersistencePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mClient.scheduleAsyncPersistence(ScheduleAsyncPersistencePRequest.newBuilder()
        .setPath(getTransportPath(path)).setOptions(options).build()), RPC_LOG,
        "ScheduleAsyncPersist", "path=%s,options=%s", path, options);
  }

  @Override
  public synchronized void startSync(final AlluxioURI path) throws AlluxioStatusException {
    retryRPC(
        () -> mClient
            .startSync(StartSyncPRequest.newBuilder().setPath(getTransportPath(path)).build()),
        RPC_LOG, "StartSync", "path=%s", path);
  }

  @Override
  public synchronized void stopSync(final AlluxioURI path) throws AlluxioStatusException {
    retryRPC(
        () -> mClient
            .stopSync(StopSyncPRequest.newBuilder().setPath(getTransportPath(path)).build()),
        RPC_LOG, "StopSync", "path=%s", path);
  }

  @Override
  public void unmount(final AlluxioURI alluxioPath) throws AlluxioStatusException {
    retryRPC(() -> mClient
        .unmount(UnmountPRequest.newBuilder().setAlluxioPath(getTransportPath(alluxioPath))
            .setOptions(UnmountPOptions.newBuilder().build()).build()),
        RPC_LOG, "Unmount", "path=%s", alluxioPath);
  }

  @Override
  public void updateUfsMode(final AlluxioURI ufsUri,
      final UpdateUfsModePOptions options) throws AlluxioStatusException {
    retryRPC(
        () -> mClient.updateUfsMode(UpdateUfsModePRequest.newBuilder()
            .setUfsPath(ufsUri.getRootPath()).setOptions(options).build()),
        RPC_LOG, "UpdateUfsMode", "ufsUri=%s,options=%s", ufsUri, options);
  }

  @Override
  public List<String> getStateLockHolders()
      throws AlluxioStatusException {
    return retryRPC(() -> {
      final ArrayList<String> result = new ArrayList<>();
      mClient.getStateLockHolders(GetStateLockHoldersPRequest.newBuilder()
          .setOptions(GetStateLockHoldersPOptions.newBuilder().build()).build()).getThreadsList()
          .forEach((thread) -> result.add(thread));
      return result;
    }, RPC_LOG, "GetStateLockHolders", "");
  }

  @Override
  public void needsSync(AlluxioURI path) throws AlluxioStatusException {
    retryRPC(
        () -> mClient.needsSync(
            NeedsSyncRequest.newBuilder().setPath(getTransportPath(path)).build()),
        RPC_LOG, "NeedsSync", "path=%s", path);
  }

  @Override
  public Optional<String> submitJob(JobRequest job) {
    connectWithRuntimeException();
    final ByteString requestBody = ByteString.copyFrom(SerializationUtils.serialize(job));
    SubmitJobPRequest request = SubmitJobPRequest
        .newBuilder()
        .setRequestBody(requestBody)
        .build();
    SubmitJobPResponse response = mClient.submitJob(request);
    return response.hasJobId() ? Optional.of(response.getJobId()) : Optional.empty();
  }

  @Override
  public boolean stopJob(JobDescription jobDescription) {
    connectWithRuntimeException();
    StopJobPResponse response = mClient.stopJob(StopJobPRequest
        .newBuilder()
        .setJobDescription(alluxio.dora.grpc.JobDescription
            .newBuilder()
            .setType(jobDescription.getType())
            .setPath(jobDescription.getPath())
            .build())
        .build());
    return response.getJobStopped();
  }

  @Override
  public String getJobProgress(JobDescription jobDescription,
      JobProgressReportFormat format, boolean verbose) {
    JobProgressPOptions.Builder options = JobProgressPOptions.newBuilder()
            .setVerbose(verbose)
            .setFormat(format);
    connectWithRuntimeException();
    GetJobProgressPResponse response = mClient.getJobProgress(
        GetJobProgressPRequest.newBuilder()
            .setJobDescription(alluxio.dora.grpc.JobDescription
            .newBuilder()
            .setType(jobDescription.getType())
            .setPath(jobDescription.getPath())
            .build())
            .setOptions(options.build())
            .build());
    return response.getProgressReport();
  }

  /**
   * Gets the path that will be transported to master.
   *
   * @param uri uri
   * @return transport path
   */
  private static String getTransportPath(AlluxioURI uri) {
    if (uri.hasScheme() && !uri.getScheme().equals(Constants.SCHEME)) {
      // Return full URI for non-Alluxio path.
      return uri.toString();
    } else {
      // Scheme-less URIs are assumed to be Alluxio paths
      // and getPath() is used to avoid string conversion.
      return uri.getPath();
    }
  }
}
