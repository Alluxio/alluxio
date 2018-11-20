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
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.ListStatusPRequest;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.MountPRequest;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.RenamePRequest;
import alluxio.grpc.ScheduleAsyncPersistencePRequest;
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
import alluxio.thrift.AlluxioService;
import alluxio.thrift.FileSystemMasterClientService;
import alluxio.util.grpc.GrpcUtils;
import alluxio.wire.SetAclAction;

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
  private FileSystemMasterClientService.Client mClient = null;
  // TODO(ggezer) review grpc client initialization
  private FileSystemMasterClientServiceGrpc.FileSystemMasterClientServiceBlockingStub mGrpcClient =
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
  protected AlluxioService.Client getClient() {
    return mClient;
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
  public synchronized void connect() {
    // TODO(adit): temp workaround
    mGrpcClient = FileSystemMasterClientServiceGrpc.newBlockingStub(mChannel);
  }

  @Override
  protected void afterConnect() {
    mClient = new FileSystemMasterClientService.Client(mProtocol);
  }

  @Override
  public synchronized List<AlluxioURI> checkConsistency(final AlluxioURI path,
      final CheckConsistencyPOptions options) throws AlluxioStatusException {
    return retryRPC(() -> {
      List<String> inconsistentPaths = mGrpcClient.checkConsistency(CheckConsistencyPRequest
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
        () -> mGrpcClient.createDirectory(CreateDirectoryPRequest.newBuilder()
            .setPath(path.getPath()).setOptions(options).build()),
        "CreateDirectory");
  }

  @Override
  public synchronized void createFile(final AlluxioURI path, final CreateFilePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mGrpcClient.createFile(CreateFilePRequest.newBuilder().setPath(path.getPath())
        .setOptions(options).build()), "CreateFile");
  }

  @Override
  public synchronized void completeFile(final AlluxioURI path, final CompleteFilePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mGrpcClient.completeFile(CompleteFilePRequest.newBuilder()
        .setPath(path.getPath()).setOptions(options).build()), "CompleteFile");
  }

  @Override
  public synchronized void delete(final AlluxioURI path, final DeletePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mGrpcClient.remove(DeletePRequest.newBuilder().setPath(path.getPath())
        .setOptions(options).build()), "Delete");
  }

  @Override
  public synchronized void free(final AlluxioURI path, final FreePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mGrpcClient.free(FreePRequest.newBuilder().setPath(path.getPath())
        .setOptions(options).build()), "Free");
  }

  @Override
  public synchronized URIStatus getStatus(final AlluxioURI path, final GetStatusPOptions options)
      throws AlluxioStatusException {
    return retryRPC(() -> new URIStatus(GrpcUtils
        .fromProto(mGrpcClient.getStatus(GetStatusPRequest.newBuilder().setPath(path.getPath())
            .setOptions(options).build()).getFileInfo())),
        "GetStatus");
  }

  @Override
  public synchronized long getNewBlockIdForFile(final AlluxioURI path)
      throws AlluxioStatusException {
    return retryRPC(
        () -> mGrpcClient
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
      for (Map.Entry<String, alluxio.grpc.MountPointInfo> entry : mGrpcClient
          .getMountTable(GetMountTablePRequest.newBuilder().build()).getMountTableMap()
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
      for (alluxio.grpc.FileInfo fileInfo : mGrpcClient.listStatus(ListStatusPRequest.newBuilder()
          .setPath(path.getPath()).setOptions(options).build())
          .getFileInfoListList()) {
        result.add(new URIStatus(GrpcUtils.fromProto(fileInfo)));
      }
      return result;
    }, "ListStatus");
  }

  @Override
  public synchronized void mount(final AlluxioURI alluxioPath, final AlluxioURI ufsPath,
      final MountPOptions options) throws AlluxioStatusException {
    retryRPC(
        () -> mGrpcClient.mount(MountPRequest.newBuilder().setAlluxioPath(alluxioPath.getPath())
            .setUfsPath(ufsPath.getPath()).setOptions(options).build()),
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
    retryRPC(() -> mGrpcClient.rename(RenamePRequest.newBuilder().setPath(src.getPath())
        .setDstPath(dst.getPath()).setOptions(options).build()), "Rename");
  }

  @Override
  public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries,
      SetAclPOptions options) throws AlluxioStatusException {
    retryRPC(() -> mGrpcClient.setAcl(
        SetAclPRequest.newBuilder().setPath(path.getPath()).setAction(GrpcUtils.toProto(action))
            .addAllEntries(entries.stream().map(GrpcUtils::toProto).collect(Collectors.toList()))
            .setOptions(options).build()),
        "SetAcl");
  }

  @Override
  public synchronized void setAttribute(final AlluxioURI path, final SetAttributePOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mGrpcClient.setAttribute(SetAttributePRequest.newBuilder()
        .setPath(path.getPath()).setOptions(options).build()), "SetAttribute");
  }

  @Override
  public synchronized void scheduleAsyncPersist(final AlluxioURI path)
      throws AlluxioStatusException {
    retryRPC(
        () -> mGrpcClient.scheduleAsyncPersistence(
            ScheduleAsyncPersistencePRequest.newBuilder().setPath(path.getPath()).build()),
        "ScheduleAsyncPersist");
  }

  @Override
  public synchronized void unmount(final AlluxioURI alluxioPath) throws AlluxioStatusException {
    retryRPC(() -> mGrpcClient
        .unmount(UnmountPRequest.newBuilder().setAlluxioPath(alluxioPath.getPath())
            .setOptions(UnmountPOptions.newBuilder().build()).build()),
        "Unmount");
  }

  @Override
  public synchronized void updateUfsMode(final AlluxioURI ufsUri,
      final UpdateUfsModePOptions options) throws AlluxioStatusException {
    retryRPC(
        () -> mGrpcClient.updateUfsMode(UpdateUfsModePRequest.newBuilder()
            .setUfsPath(ufsUri.getRootPath()).setOptions(options).build()),
        "UpdateUfsMode");
  }
}
