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
import alluxio.client.file.options.CheckConsistencyOptions;
import alluxio.client.file.options.CompleteFileOptions;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.client.file.options.RenameOptions;
import alluxio.client.file.options.SetAclOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.client.file.options.UpdateUfsModeOptions;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.CheckConsistencyPRequest;
import alluxio.grpc.CompleteFilePRequest;
import alluxio.grpc.CreateDirectoryPRequest;
import alluxio.grpc.CreateFilePRequest;
import alluxio.grpc.DeletePRequest;
import alluxio.grpc.FreePRequest;
import alluxio.grpc.GetMountTablePRequest;
import alluxio.grpc.GetNewBlockIdForFilePOptions;
import alluxio.grpc.GetNewBlockIdForFilePRequest;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.ListStatusPRequest;
import alluxio.grpc.MountPRequest;
import alluxio.grpc.RenamePRequest;
import alluxio.grpc.ScheduleAsyncPersistencePRequest;
import alluxio.grpc.SetAttributePRequest;
import alluxio.grpc.UnmountPOptions;
import alluxio.grpc.UnmountPRequest;
import alluxio.grpc.UpdateUfsModePRequest;
import alluxio.master.MasterClientConfig;
import alluxio.security.authorization.AclEntry;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.FileSystemMasterClientService;
<<<<<<< HEAD
import alluxio.util.grpc.GrpcUtils;
||||||| merged common ancestors
import alluxio.thrift.GetMountTableTResponse;
import alluxio.thrift.GetNewBlockIdForFileTOptions;
import alluxio.thrift.LoadMetadataTOptions;
import alluxio.thrift.ScheduleAsyncPersistenceTOptions;
import alluxio.thrift.UnmountTOptions;
import alluxio.wire.ThriftUtils;

import org.apache.thrift.TException;
=======
import alluxio.thrift.GetMountTableTResponse;
import alluxio.thrift.GetNewBlockIdForFileTOptions;
import alluxio.thrift.LoadMetadataTOptions;
import alluxio.thrift.ScheduleAsyncPersistenceTOptions;
import alluxio.thrift.UnmountTOptions;
import alluxio.wire.FileInfo;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SetAclAction;
>>>>>>> master

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
  }

  @Override
  protected void afterConnect() {
    mClient = new FileSystemMasterClientService.Client(mProtocol);
  }

  @Override
  public synchronized List<AlluxioURI> checkConsistency(final AlluxioURI path,
      final CheckConsistencyOptions options) throws AlluxioStatusException {
    return retryRPC(() -> {
      List<String> inconsistentPaths = mBlockingStub.checkConsistency(CheckConsistencyPRequest
          .newBuilder().setPath(path.getPath()).setOptions(GrpcUtils.toProto(options)).build())
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
      final CreateDirectoryOptions options) throws AlluxioStatusException {
<<<<<<< HEAD
    retryRPC(() -> {
      mBlockingStub.createDirectory(CreateDirectoryPRequest.newBuilder().setPath(path.getPath())
          .setOptions(GrpcUtils.toProto(options)).build());
      return null;
    });
||||||| merged common ancestors
    retryRPC(() -> {
      mClient.createDirectory(path.getPath(), options.toThrift());
      return null;
    });
=======
    retryRPC(() -> mClient.createDirectory(path.getPath(), options.toThrift()), "CreateDirectory");
>>>>>>> master
  }

  @Override
  public synchronized void createFile(final AlluxioURI path, final CreateFileOptions options)
      throws AlluxioStatusException {
<<<<<<< HEAD
    retryRPC(() -> {
      mBlockingStub.createFile(CreateFilePRequest.newBuilder().setPath(path.getPath())
          .setOptions(GrpcUtils.toProto(options)).build());
      return null;
    });
||||||| merged common ancestors
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.createFile(path.getPath(), options.toThrift());
        return null;
      }
    });
=======
    retryRPC(() -> mClient.createFile(path.getPath(), options.toThrift()), "CreateFile");
>>>>>>> master
  }

  @Override
  public synchronized void completeFile(final AlluxioURI path, final CompleteFileOptions options)
      throws AlluxioStatusException {
<<<<<<< HEAD
    retryRPC(() -> {
      mBlockingStub.completeFile(CompleteFilePRequest.newBuilder().setPath(path.getPath())
          .setOptions(GrpcUtils.toProto(options)).build());
      return null;
    });
||||||| merged common ancestors
    retryRPC(() -> {
      mClient.completeFile(path.getPath(), options.toThrift());
      return null;
    });
=======
    retryRPC(() -> mClient.completeFile(path.getPath(), options.toThrift()), "CompleteFile");
>>>>>>> master
  }

  @Override
  public synchronized void delete(final AlluxioURI path, final DeleteOptions options)
      throws AlluxioStatusException {
<<<<<<< HEAD
    retryRPC(() -> {
      mBlockingStub.remove(DeletePRequest.newBuilder().setPath(path.getPath())
          .setOptions(GrpcUtils.toProto(options)).build());
      return null;
    });
||||||| merged common ancestors
    retryRPC(() -> {
      mClient.remove(path.getPath(), options.isRecursive(), options.toThrift());
      return null;
    });
=======
    retryRPC(() -> mClient.remove(path.getPath(), options.isRecursive(), options.toThrift()),
        "Delete");
>>>>>>> master
  }

  @Override
  public synchronized void free(final AlluxioURI path, final FreeOptions options)
      throws AlluxioStatusException {
<<<<<<< HEAD
    retryRPC(() -> {
      mBlockingStub.free(FreePRequest.newBuilder().setPath(path.getPath())
          .setOptions(GrpcUtils.toProto(options)).build());
      return null;
    });
||||||| merged common ancestors
    retryRPC(() -> {
      mClient.free(path.getPath(), options.isRecursive(), options.toThrift());
      return null;
    });
=======
    retryRPC(() -> mClient.free(path.getPath(), options.isRecursive(), options.toThrift()), "Free");
>>>>>>> master
  }

  @Override
  public synchronized URIStatus getStatus(final AlluxioURI path, final GetStatusOptions options)
      throws AlluxioStatusException {
<<<<<<< HEAD
    return retryRPC(() -> new URIStatus(GrpcUtils
        .fromProto(mBlockingStub.getStatus(GetStatusPRequest.newBuilder().setPath(path.getPath())
            .setOptions(GrpcUtils.toProto(options)).build()).getFileInfo())));
||||||| merged common ancestors
    return retryRPC(() -> new URIStatus(ThriftUtils
            .fromThrift(mClient.getStatus(path.getPath(), options.toThrift()).getFileInfo())));
=======
    return retryRPC(() -> new URIStatus(FileInfo.fromThrift(mClient.getStatus(path.getPath(),
        options.toThrift()).getFileInfo())), "GetStatus");
>>>>>>> master
  }

  @Override
  public synchronized long getNewBlockIdForFile(final AlluxioURI path)
      throws AlluxioStatusException {
<<<<<<< HEAD
    return retryRPC(() -> mBlockingStub
        .getNewBlockIdForFile(GetNewBlockIdForFilePRequest.newBuilder().setPath(path.getPath())
            .setOptions(GetNewBlockIdForFilePOptions.newBuilder().build()).build())
        .getId());
||||||| merged common ancestors
    return retryRPC(
        () -> mClient.getNewBlockIdForFile(path.getPath(), new GetNewBlockIdForFileTOptions())
            .getId());
=======
    return retryRPC(() -> mClient.getNewBlockIdForFile(path.getPath(),
        new GetNewBlockIdForFileTOptions()).getId(), "GetNewBlockIdForFile");
>>>>>>> master
  }

  @Override
  public synchronized Map<String, alluxio.wire.MountPointInfo> getMountTable()
      throws AlluxioStatusException {
    return retryRPC(() -> {
      Map<String, alluxio.wire.MountPointInfo> mountTableWire = new HashMap<>();
<<<<<<< HEAD
      for (Map.Entry<String, alluxio.grpc.MountPointInfo> entry : mBlockingStub
          .getMountTable(GetMountTablePRequest.newBuilder().build()).getMountTableMap()
          .entrySet()) {
        mountTableWire.put(entry.getKey(), GrpcUtils.fromProto(entry.getValue()));
||||||| merged common ancestors
      for (Map.Entry<String, alluxio.thrift.MountPointInfo> entry : mountTableThrift.entrySet()) {
        alluxio.thrift.MountPointInfo mMountPointInfoThrift = entry.getValue();
        alluxio.wire.MountPointInfo mMountPointInfoWire =
            ThriftUtils.fromThrift(mMountPointInfoThrift);
        mountTableWire.put(entry.getKey(), mMountPointInfoWire);
=======
      for (Map.Entry<String, alluxio.thrift.MountPointInfo> entry : mountTableThrift.entrySet()) {
        alluxio.thrift.MountPointInfo mMountPointInfoThrift = entry.getValue();
        alluxio.wire.MountPointInfo mMountPointInfoWire =
            MountPointInfo.fromThrift(mMountPointInfoThrift);
        mountTableWire.put(entry.getKey(), mMountPointInfoWire);
>>>>>>> master
      }
      return mountTableWire;
    }, "GetMountTable");
  }

  @Override
  public synchronized List<URIStatus> listStatus(final AlluxioURI path,
      final ListStatusOptions options) throws AlluxioStatusException {
    return retryRPC(() -> {
      List<URIStatus> result = new ArrayList<>();
<<<<<<< HEAD
      for (alluxio.grpc.FileInfo fileInfo : mBlockingStub.listStatus(ListStatusPRequest.newBuilder()
          .setPath(path.getPath()).setOptions(GrpcUtils.toProto(options)).build())
          .getFileInfoListList()) {
        result.add(new URIStatus(GrpcUtils.fromProto(fileInfo)));
||||||| merged common ancestors
      for (alluxio.thrift.FileInfo fileInfo : mClient.listStatus(path.getPath(), options.toThrift())
          .getFileInfoList()) {
        result.add(new URIStatus(ThriftUtils.fromThrift(fileInfo)));
=======
      for (alluxio.thrift.FileInfo fileInfo : mClient
          .listStatus(path.getPath(), options.toThrift()).getFileInfoList()) {
        result.add(new URIStatus(FileInfo.fromThrift(fileInfo)));
>>>>>>> master
      }
      return result;
    }, "ListStatus");
  }

  @Override
<<<<<<< HEAD
||||||| merged common ancestors
  public synchronized void loadMetadata(final AlluxioURI path,
      final LoadMetadataOptions options) throws AlluxioStatusException {
    retryRPC(() -> {
      return mClient
            .loadMetadata(path.toString(), options.isRecursive(), new LoadMetadataTOptions())
            .getId();
    });
  }

  @Override
=======
  public synchronized void loadMetadata(final AlluxioURI path, final LoadMetadataOptions options)
      throws AlluxioStatusException {
    retryRPC(() -> mClient.loadMetadata(path.toString(), options.isRecursive(),
          new LoadMetadataTOptions()).getId(), "LoadMetadata");
  }

  @Override
>>>>>>> master
  public synchronized void mount(final AlluxioURI alluxioPath, final AlluxioURI ufsPath,
      final MountOptions options) throws AlluxioStatusException {
<<<<<<< HEAD
    retryRPC(() -> {
      mBlockingStub.mount(MountPRequest.newBuilder().setAlluxioPath(alluxioPath.getPath())
          .setUfsPath(ufsPath.getPath()).setOptions(GrpcUtils.toProto(options)).build());
      return null;
    });
||||||| merged common ancestors
    retryRPC(() -> {
      mClient.mount(alluxioPath.toString(), ufsPath.toString(), options.toThrift());
      return null;
    });
=======
    retryRPC(() -> mClient.mount(alluxioPath.toString(), ufsPath.toString(), options.toThrift()),
        "Mount");
>>>>>>> master
  }

  @Override
  public synchronized void rename(final AlluxioURI src, final AlluxioURI dst)
      throws AlluxioStatusException {
    rename(src, dst, RenameOptions.defaults());
  }

  @Override
  public synchronized void rename(final AlluxioURI src, final AlluxioURI dst,
      final RenameOptions options) throws AlluxioStatusException {
    retryRPC(() -> mClient.rename(src.getPath(), dst.getPath(), options.toThrift()), "Rename");
  }

  @Override
  public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries,
      SetAclOptions options) throws AlluxioStatusException {
    retryRPC(() -> {
<<<<<<< HEAD
      mBlockingStub.rename(RenamePRequest.newBuilder().setPath(src.getPath())
          .setDstPath(dst.getPath()).setOptions(GrpcUtils.toProto(options)).build());
||||||| merged common ancestors
      mClient.rename(src.getPath(), dst.getPath(), options.toThrift());
=======
      mClient.setAcl(path.getPath(), action.toThrift(),
          entries.stream().map(AclEntry::toThrift).collect(Collectors.toList()),
          options.toThrift());
>>>>>>> master
      return null;
    });
  }

  @Override
  public synchronized void setAttribute(final AlluxioURI path, final SetAttributeOptions options)
      throws AlluxioStatusException {
<<<<<<< HEAD
    retryRPC(() -> {
      mBlockingStub.setAttribute(SetAttributePRequest.newBuilder().setPath(path.getPath())
          .setOptions(GrpcUtils.toProto(options)).build());
      return null;
    });
||||||| merged common ancestors
    retryRPC(() -> {
      mClient.setAttribute(path.getPath(), options.toThrift());
      return null;
    });
=======
    retryRPC(() -> mClient.setAttribute(path.getPath(), options.toThrift()), "SetAttribute");
>>>>>>> master
  }

  @Override
  public synchronized void scheduleAsyncPersist(final AlluxioURI path)
      throws AlluxioStatusException {
<<<<<<< HEAD
    retryRPC(() -> {
      mBlockingStub.scheduleAsyncPersistence(
          ScheduleAsyncPersistencePRequest.newBuilder().setPath(path.getPath()).build());
      return null;
    });
||||||| merged common ancestors
    retryRPC(() -> {
      mClient.scheduleAsyncPersistence(path.getPath(), new ScheduleAsyncPersistenceTOptions());
      return null;
    });
=======
    retryRPC(() -> mClient.scheduleAsyncPersistence(path.getPath(),
        new ScheduleAsyncPersistenceTOptions()), "ScheduleAsyncPersist");
>>>>>>> master
  }

  @Override
  public synchronized void unmount(final AlluxioURI alluxioPath) throws AlluxioStatusException {
<<<<<<< HEAD
    retryRPC(() -> {
      mBlockingStub.unmount(UnmountPRequest.newBuilder().setAlluxioPath(alluxioPath.getPath())
          .setOptions(UnmountPOptions.newBuilder().build()).build());
      return null;
    });
||||||| merged common ancestors
    retryRPC(() -> {
      mClient.unmount(alluxioPath.toString(), new UnmountTOptions());
      return null;
    });
=======
    retryRPC(() -> mClient.unmount(alluxioPath.toString(), new UnmountTOptions()), "Unmount");
>>>>>>> master
  }

  @Override
  public synchronized void updateUfsMode(final AlluxioURI ufsUri,
      final UpdateUfsModeOptions options) throws AlluxioStatusException {
<<<<<<< HEAD
    retryRPC(() -> {
      mBlockingStub.updateUfsMode(UpdateUfsModePRequest.newBuilder()
          .setUfsPath(ufsUri.getRootPath()).setOptions(GrpcUtils.toProto(options)).build());
      return null;
    });
||||||| merged common ancestors
    retryRPC(() -> {
      mClient.updateUfsMode(ufsUri.getRootPath(), options.toThrift());
      return null;
    });
=======
    retryRPC(() -> mClient.updateUfsMode(ufsUri.getRootPath(), options.toThrift()),
        "UpdateUfsMode");
>>>>>>> master
  }
}
