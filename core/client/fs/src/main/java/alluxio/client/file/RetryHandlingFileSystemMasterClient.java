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

package alluxio.client.file;

import alluxio.AbstractMasterClient;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.AlluxioServiceType;
import alluxio.grpc.CheckConsistencyPOptions;
import alluxio.grpc.CheckConsistencyPRequest;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CompleteFilePRequest;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateDirectoryPRequest;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.CreateFilePRequest;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.DeletePRequest;
import alluxio.grpc.FileSystemMasterClientServiceGrpc;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.FreePRequest;
import alluxio.grpc.GetMountTablePRequest;
import alluxio.grpc.GetNewBlockIdForFilePOptions;
import alluxio.grpc.GetNewBlockIdForFilePRequest;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.ListStatusPRequest;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.MountPRequest;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.RenamePRequest;
import alluxio.grpc.ScheduleAsyncPersistencePRequest;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAclPRequest;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.SetAttributePRequest;
import alluxio.grpc.UnmountPOptions;
import alluxio.grpc.UnmountPRequest;
import alluxio.grpc.UpdateUfsModePOptions;
import alluxio.grpc.UpdateUfsModePRequest;
import alluxio.master.MasterClientConfig;
import alluxio.security.authorization.AclEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the thrift client to interact with the file system master, used by alluxio clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class RetryHandlingFileSystemMasterClient extends AbstractMasterClient
    implements FileSystemMasterClient {
  private FileSystemMasterClientServiceGrpc.FileSystemMasterClientServiceBlockingStub mClient =
      null;

  /**
   * Creates a new {@link RetryHandlingFileSystemMasterClient} instance.
   *
   * @param conf master client configuration
   */
  public RetryHandlingFileSystemMasterClient(MasterClientConfig conf) {
    super(conf);
  }

  @Override
  protected AlluxioServiceType getRemoteServiceType() {
    return AlluxioServiceType.FILE_SYSTEM_MASTER_CLIENT_SERVICE;
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
  public synchronized List<AlluxioURI> checkConsistency(final AlluxioURI path,
      final CheckConsistencyPOptions options) throws AlluxioStatusException {
    return retryRPC(() -> {
      List<String> inconsistentPaths = mClient.checkConsistency(CheckConsistencyPRequest
          .newBuilder().setPath(path.getPath()).setOptions(options).build())
          .getInconsistentPathsList();
      List<AlluxioURI> inconsistentUris = new ArrayList<>(inconsistentPaths.size());
      for (String inconsistentPath : inconsistentPaths) {
        inconsistentUris.add(new AlluxioURI(inconsistentPath));
      }
      return inconsistentUris;
    }, "CheckConsistency");
  }

  @Override
  public synchronized void createDirectory(final AlluxioURI path,
      final CreateDirectoryPOptions options) throws AlluxioStatusException {
    retryRPC(
        () -> mClient.createDirectory(CreateDirectoryPRequest.newBuilder()
            .setPath(path.getPath()).setOptions(options).build()),
        "CreateDirectory");
  }

  @Override
  public synchronized void createFile(final AlluxioURI path, final CreateFilePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mClient.createFile(CreateFilePRequest.newBuilder().setPath(path.getPath())
        .setOptions(options).build()), "CreateFile");
  }

  @Override
  public synchronized void completeFile(final AlluxioURI path, final CompleteFilePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mClient.completeFile(CompleteFilePRequest.newBuilder()
        .setPath(path.getPath()).setOptions(options).build()), "CompleteFile");
  }

  @Override
  public synchronized void delete(final AlluxioURI path, final DeletePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mClient.remove(DeletePRequest.newBuilder().setPath(path.getPath())
        .setOptions(options).build()), "Delete");
  }

  @Override
  public synchronized void free(final AlluxioURI path, final FreePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mClient.free(FreePRequest.newBuilder().setPath(path.getPath())
        .setOptions(options).build()), "Free");
  }

  @Override
  public synchronized URIStatus getStatus(final AlluxioURI path, final GetStatusPOptions options)
      throws AlluxioStatusException {
    return retryRPC(() -> new URIStatus(GrpcUtils
        .fromProto(mClient.getStatus(GetStatusPRequest.newBuilder().setPath(path.getPath())
            .setOptions(options).build()).getFileInfo())),
        "GetStatus");
  }

  @Override
  public synchronized long getNewBlockIdForFile(final AlluxioURI path)
      throws AlluxioStatusException {
    return retryRPC(
        () -> mClient
            .getNewBlockIdForFile(GetNewBlockIdForFilePRequest.newBuilder().setPath(path.getPath())
                .setOptions(GetNewBlockIdForFilePOptions.newBuilder().build()).build())
            .getId(),
        "GetNewBlockIdForFile");
  }

  @Override
  public synchronized Map<String, alluxio.wire.MountPointInfo> getMountTable()
      throws AlluxioStatusException {
    return retryRPC(() -> {
      Map<String, alluxio.wire.MountPointInfo> mountTableWire = new HashMap<>();
      for (Map.Entry<String, alluxio.grpc.MountPointInfo> entry : mClient
          .getMountTable(GetMountTablePRequest.newBuilder().build()).getMountPointsMap()
          .entrySet()) {
        mountTableWire.put(entry.getKey(), GrpcUtils.fromProto(entry.getValue()));
      }
      return mountTableWire;
    }, "GetMountTable");
  }

  @Override
  public synchronized List<URIStatus> listStatus(final AlluxioURI path,
      final ListStatusPOptions options) throws AlluxioStatusException {
    return retryRPC(() -> {
      List<URIStatus> result = new ArrayList<>();
      for (alluxio.grpc.FileInfo fileInfo : mClient.listStatus(ListStatusPRequest.newBuilder()
          .setPath(path.getPath()).setOptions(options).build())
          .getFileInfosList()) {
        result.add(new URIStatus(GrpcUtils.fromProto(fileInfo)));
      }
      return result;
    }, "ListStatus");
  }

  @Override
  public synchronized void mount(final AlluxioURI alluxioPath, final AlluxioURI ufsPath,
      final MountPOptions options) throws AlluxioStatusException {
    retryRPC(
        () -> mClient.mount(MountPRequest.newBuilder().setAlluxioPath(alluxioPath.toString())
            .setUfsPath(ufsPath.toString()).setOptions(options).build()),
        "Mount");
  }

  @Override
  public synchronized void rename(final AlluxioURI src, final AlluxioURI dst)
      throws AlluxioStatusException {
    rename(src, dst, FileSystemClientOptions.getRenameOptions());
  }

  @Override
  public synchronized void rename(final AlluxioURI src, final AlluxioURI dst,
      final RenamePOptions options) throws AlluxioStatusException {
    retryRPC(() -> mClient.rename(RenamePRequest.newBuilder().setPath(src.getPath())
        .setDstPath(dst.getPath()).setOptions(options).build()), "Rename");
  }

  @Override
  public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries,
      SetAclPOptions options) throws AlluxioStatusException {
    retryRPC(() -> mClient.setAcl(
        SetAclPRequest.newBuilder().setPath(path.getPath()).setAction(action)
            .addAllEntries(entries.stream().map(GrpcUtils::toProto).collect(Collectors.toList()))
            .setOptions(options).build()),
        "SetAcl");
  }

  @Override
  public synchronized void setAttribute(final AlluxioURI path, final SetAttributePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mClient.setAttribute(SetAttributePRequest.newBuilder()
        .setPath(path.getPath()).setOptions(options).build()), "SetAttribute");
  }

  @Override
  public synchronized void scheduleAsyncPersist(final AlluxioURI path)
      throws AlluxioStatusException {
    retryRPC(
        () -> mClient.scheduleAsyncPersistence(
            ScheduleAsyncPersistencePRequest.newBuilder().setPath(path.getPath()).build()),
        "ScheduleAsyncPersist");
  }

  @Override
  public synchronized void unmount(final AlluxioURI alluxioPath) throws AlluxioStatusException {
    retryRPC(() -> mClient
        .unmount(UnmountPRequest.newBuilder().setAlluxioPath(alluxioPath.toString())
            .setOptions(UnmountPOptions.newBuilder().build()).build()),
        "Unmount");
  }

  @Override
  public synchronized void updateUfsMode(final AlluxioURI ufsUri,
      final UpdateUfsModePOptions options) throws AlluxioStatusException {
    retryRPC(
        () -> mClient.updateUfsMode(UpdateUfsModePRequest.newBuilder()
            .setUfsPath(ufsUri.getRootPath()).setOptions(options).build()),
        "UpdateUfsMode");
  }
}
