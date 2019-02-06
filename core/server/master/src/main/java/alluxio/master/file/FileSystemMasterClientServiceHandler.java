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
import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.RpcUtils.RpcCallable;
import alluxio.RpcUtils.RpcCallableThrowsIOException;
import alluxio.master.file.options.CheckConsistencyOptions;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.DeleteOptions;
import alluxio.master.file.options.DescendantType;
import alluxio.master.file.options.FreeOptions;
import alluxio.master.file.options.GetStatusOptions;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.master.file.options.LoadMetadataOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.master.file.options.RenameOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.CheckConsistencyTOptions;
import alluxio.thrift.CheckConsistencyTResponse;
import alluxio.thrift.CompleteFileTOptions;
import alluxio.thrift.CompleteFileTResponse;
import alluxio.thrift.CreateDirectoryTOptions;
import alluxio.thrift.CreateDirectoryTResponse;
import alluxio.thrift.CreateFileTOptions;
import alluxio.thrift.CreateFileTResponse;
import alluxio.thrift.DeleteTOptions;
import alluxio.thrift.DeleteTResponse;
import alluxio.thrift.FileInfo;
import alluxio.thrift.FileSystemMasterClientService;
import alluxio.thrift.FreeTOptions;
import alluxio.thrift.FreeTResponse;
import alluxio.thrift.GetFilePathTOptions;
import alluxio.thrift.GetFilePathTResponse;
import alluxio.thrift.GetMountTableTResponse;
import alluxio.thrift.GetNewBlockIdForFileTOptions;
import alluxio.thrift.GetNewBlockIdForFileTResponse;
import alluxio.thrift.GetServiceVersionTOptions;
import alluxio.thrift.GetServiceVersionTResponse;
import alluxio.thrift.GetStatusTOptions;
import alluxio.thrift.GetStatusTResponse;
import alluxio.thrift.ListStatusTOptions;
import alluxio.thrift.ListStatusTResponse;
import alluxio.thrift.LoadMetadataTOptions;
import alluxio.thrift.LoadMetadataTResponse;
import alluxio.thrift.MountTOptions;
import alluxio.thrift.MountTResponse;
import alluxio.thrift.RenameTOptions;
import alluxio.thrift.RenameTResponse;
import alluxio.thrift.ScheduleAsyncPersistenceTOptions;
import alluxio.thrift.ScheduleAsyncPersistenceTResponse;
import alluxio.thrift.SetAttributeTOptions;
import alluxio.thrift.SetAttributeTResponse;
import alluxio.thrift.UnmountTOptions;
import alluxio.thrift.UnmountTResponse;
import alluxio.thrift.UpdateUfsModeTOptions;
import alluxio.thrift.UpdateUfsModeTResponse;
import alluxio.underfs.UnderFileSystem;
import alluxio.wire.MountPointInfo;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is a Thrift handler for file system master RPCs invoked by an Alluxio client.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class FileSystemMasterClientServiceHandler implements
    FileSystemMasterClientService.Iface {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemMasterClientServiceHandler.class);
  private final FileSystemMaster mFileSystemMaster;

  /**
   * Creates a new instance of {@link FileSystemMasterClientServiceHandler}.
   *
   * @param fileSystemMaster the {@link FileSystemMaster} the handler uses internally
   */
  FileSystemMasterClientServiceHandler(FileSystemMaster fileSystemMaster) {
    Preconditions.checkNotNull(fileSystemMaster, "fileSystemMaster");
    mFileSystemMaster = fileSystemMaster;
  }

  @Override
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options) {
    return new GetServiceVersionTResponse(Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION);
  }

  @Override
  public CheckConsistencyTResponse checkConsistency(final String path,
      final CheckConsistencyTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallableThrowsIOException<CheckConsistencyTResponse>) () -> {
      List<AlluxioURI> inconsistentUris = mFileSystemMaster.checkConsistency(new AlluxioURI(path),
          new CheckConsistencyOptions(options));
      List<String> uris = new ArrayList<>(inconsistentUris.size());
      for (AlluxioURI uri : inconsistentUris) {
        uris.add(uri.getPath());
      }
      return new CheckConsistencyTResponse(uris);
    }, "CheckConsistency", "path=%s, options=%s", path, options);
  }

  @Override
  public CompleteFileTResponse completeFile(final String path, final CompleteFileTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallableThrowsIOException<CompleteFileTResponse>) () -> {
      mFileSystemMaster.completeFile(new AlluxioURI(path), new CompleteFileOptions(options));
      return new CompleteFileTResponse();
    }, "CompleteFile", "path=%s, options=%s", path, options);
  }

  @Override
  public CreateDirectoryTResponse createDirectory(final String path,
      final CreateDirectoryTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallableThrowsIOException<CreateDirectoryTResponse>) () -> {
      mFileSystemMaster.createDirectory(new AlluxioURI(path), new CreateDirectoryOptions(options));
      return new CreateDirectoryTResponse();
    }, "CreateDirectory", "path=%s, options=%s", path, options);
  }

  @Override
  public CreateFileTResponse createFile(final String path, final CreateFileTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallableThrowsIOException<CreateFileTResponse>) () -> {
      mFileSystemMaster.createFile(new AlluxioURI(path), new CreateFileOptions(options));
      return new CreateFileTResponse();
    }, "CreateFile", "path=%s, options=%s", path, options);
  }

  @Override
  public FreeTResponse free(final String path, final boolean recursive, final FreeTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallableThrowsIOException<FreeTResponse>) () -> {
      if (options == null) {
        // For Alluxio client v1.4 or earlier.
        // NOTE, we try to be conservative here so early Alluxio clients will not be able to force
        // freeing pinned items but see the error thrown.
        mFileSystemMaster
            .free(new AlluxioURI(path), FreeOptions.defaults().setRecursive(recursive));
      } else {
        mFileSystemMaster.free(new AlluxioURI(path), new FreeOptions(options));
      }
      return new FreeTResponse();
    }, "Free", "path=%s, recursive=%s, options=%s", path, recursive, options);
  }

  @Override
  public GetNewBlockIdForFileTResponse getNewBlockIdForFile(final String path,
      final GetNewBlockIdForFileTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG,
        (RpcCallable<GetNewBlockIdForFileTResponse>) () -> new GetNewBlockIdForFileTResponse(
            mFileSystemMaster.getNewBlockIdForFile(new AlluxioURI(path))), "GetNewBlockIdForFile",
        "path=%s, options=%s", path, options);
  }

  @Override
  public GetFilePathTResponse getFilePath(final GetFilePathTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG,
        (RpcCallableThrowsIOException<GetFilePathTResponse>) () -> new GetFilePathTResponse(
            mFileSystemMaster.getPath(options.getFileId()).toString()),
        // getStatus is often used to check file existence, so we avoid logging all of its failures
        "GetFilePath", true, "options=%s", options);
  }

  @Override
  public GetStatusTResponse getStatus(final String path, final GetStatusTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG,
        (RpcCallableThrowsIOException<GetStatusTResponse>) () -> new GetStatusTResponse(
            mFileSystemMaster.getFileInfo(new AlluxioURI(path), new GetStatusOptions(options))
                .toThrift()),
        // getStatus is often used to check file existence, so we avoid logging all of its failures
        "GetStatus", true, "path=%s, options=%s", path, options);
  }

  @Override
  public ListStatusTResponse listStatus(final String path, final ListStatusTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallableThrowsIOException<ListStatusTResponse>) () -> {
      List<FileInfo> result = new ArrayList<>();
      for (alluxio.wire.FileInfo fileInfo : mFileSystemMaster.listStatus(new AlluxioURI(path),
          new ListStatusOptions(options))) {
        result.add(fileInfo.toThrift());
      }
      return new ListStatusTResponse(result);
    }, "ListStatus", "path=%s, options=%s", path, options);
  }

  /**
   * {@inheritDoc}
   *
   * @deprecated since version 1.1 and will be removed in version 2.0
   */
  @Override
  @Deprecated
  public LoadMetadataTResponse loadMetadata(final String alluxioPath, final boolean recursive,
      final LoadMetadataTOptions options) throws AlluxioTException {
    return RpcUtils.call(
        LOG,
        (RpcCallableThrowsIOException<LoadMetadataTResponse>) () -> new LoadMetadataTResponse(
            mFileSystemMaster.loadMetadata(new AlluxioURI(alluxioPath), LoadMetadataOptions
                .defaults().setCreateAncestors(true).setLoadDescendantType(DescendantType.ONE))),
        "LoadMetadata", "alluxioPath=%s, recursive=%s, options=%s", alluxioPath,
        recursive, options);
  }

  @Override
  public MountTResponse mount(final String alluxioPath, final String ufsPath,
      final MountTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallableThrowsIOException<MountTResponse>) () -> {
      mFileSystemMaster.mount(new AlluxioURI(alluxioPath), new AlluxioURI(ufsPath),
          new MountOptions(options));
      return new MountTResponse();
    }, "Mount", "alluxioPath=%s, ufsPath=%s, options=%s", alluxioPath, ufsPath, options);
  }

  @Override
  public GetMountTableTResponse getMountTable() throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallableThrowsIOException<GetMountTableTResponse>) () -> {
      Map<String, MountPointInfo> mountTableWire = mFileSystemMaster.getMountTable();
      Map<String, alluxio.thrift.MountPointInfo> mountTableThrift = new HashMap<>();
      for (Map.Entry<String, MountPointInfo> entry : mountTableWire.entrySet()) {
        MountPointInfo mMountPointInfo = entry.getValue();
        alluxio.thrift.MountPointInfo mountPointThrift = mMountPointInfo.toThrift();
        mountTableThrift.put(entry.getKey(), mountPointThrift);
      }
      return new GetMountTableTResponse(mountTableThrift);
    }, "GetMountTable", "");
  }

  @Override
  public DeleteTResponse remove(final String path, final boolean recursive,
      final DeleteTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallableThrowsIOException<DeleteTResponse>) () -> {
      if (options == null) {
        // For Alluxio client v1.4 or earlier.
        // NOTE, we try to be conservative here so early Alluxio clients will not be able to
        // delete files in Alluxio only.
        mFileSystemMaster.delete(new AlluxioURI(path),
            DeleteOptions.defaults().setRecursive(recursive).setUnchecked(options.isUnchecked()));
      } else {
        mFileSystemMaster.delete(new AlluxioURI(path), new DeleteOptions(options));
      }
      return new DeleteTResponse();
    }, "Remove", "path=%s, recursive=%s, options=%s", path, recursive, options);
  }

  @Override
  public RenameTResponse rename(final String srcPath, final String dstPath,
      final RenameTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallableThrowsIOException<RenameTResponse>) () -> {
      mFileSystemMaster.rename(new AlluxioURI(srcPath), new AlluxioURI(dstPath), new RenameOptions(
          options));
      return new RenameTResponse();
    }, "Rename", "srcPath=%s, dstPath=%s, options=%s", srcPath, dstPath, options);
  }

  @Override
  public ScheduleAsyncPersistenceTResponse scheduleAsyncPersistence(final String path,
      final ScheduleAsyncPersistenceTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG,
        (RpcCallableThrowsIOException<ScheduleAsyncPersistenceTResponse>) () -> {
          mFileSystemMaster.scheduleAsyncPersistence(new AlluxioURI(path));
          return new ScheduleAsyncPersistenceTResponse();
        }, "ScheduleAsyncPersist", "path=%s, options=%s", path, options);
  }

  @Override
  public SetAttributeTResponse setAttribute(final String path, final SetAttributeTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallableThrowsIOException<SetAttributeTResponse>) () -> {
      mFileSystemMaster.setAttribute(new AlluxioURI(path), new SetAttributeOptions(options));
      return new SetAttributeTResponse();
    }, "SetAttribute", "path=%s, options=%s", path, options);
  }

  @Override
  public UnmountTResponse unmount(final String alluxioPath, final UnmountTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallableThrowsIOException<UnmountTResponse>) () -> {
      mFileSystemMaster.unmount(new AlluxioURI(alluxioPath));
      return new UnmountTResponse();
    }, "Unmount", "alluxioPath=%s, options=%s", alluxioPath, options);
  }

  @Override
  public UpdateUfsModeTResponse updateUfsMode(final String ufsPath,
      final UpdateUfsModeTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, (RpcCallableThrowsIOException<UpdateUfsModeTResponse>) () -> {
      UnderFileSystem.UfsMode ufsMode;
      switch (options.getUfsMode()) {
        case NoAccess:
          ufsMode = UnderFileSystem.UfsMode.NO_ACCESS;
          break;
        case ReadOnly:
          ufsMode = UnderFileSystem.UfsMode.READ_ONLY;
          break;
        default:
          ufsMode = UnderFileSystem.UfsMode.READ_WRITE;
          break;
      }
      mFileSystemMaster.updateUfsMode(new AlluxioURI(ufsPath), ufsMode);
      return new UpdateUfsModeTResponse();
    }, "UpdateUfsMode", "ufsPath=%s, options=%s", ufsPath, options);
  }
}
