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

package alluxio.client.file.ufs;

import alluxio.AlluxioURI;
import alluxio.PositionReader;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.ListStatusPartialResult;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.UfsFileSystemOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.grpc.CheckAccessPOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ErrorType;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.JobProgressReportFormat;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.ListStatusPartialPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UnmountPOptions;
import alluxio.job.JobDescription;
import alluxio.job.JobRequest;
import alluxio.resource.CloseableResource;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.Mode;
import alluxio.underfs.Fingerprint;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.GetStatusOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;
import alluxio.util.ModeUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.MountPointInfo;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;

/**
 * An implementation of the {@link FileSystem} interface that directly interacts with a target
 * UFS. No Alluxio server is involved.
 */
@ThreadSafe
public class UfsBaseFileSystem implements FileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(UfsBaseFileSystem.class);

  /** Used to manage closeable resources. */
  private final Closer mCloser = Closer.create();
  protected final FileSystemContext mFsContext;
  protected final CloseableResource<UnderFileSystem> mUfs;
  private final AlluxioURI mRootUFS;
  protected volatile boolean mClosed = false;

  /**
   * Constructs a new base file system.
   *
   * @param fsContext file system context
   * @param options the ufs file system options
   */
  public UfsBaseFileSystem(FileSystemContext fsContext, UfsFileSystemOptions options) {
    this(fsContext, options, new UfsManager.UfsClient(
        () -> UnderFileSystem.Factory.create(options.getUfsAddress(), fsContext.getClusterConf()),
        new AlluxioURI(options.getUfsAddress())));
    mCloser.register(mFsContext);
    LOG.debug("Creating file system connecting to ufs address {}", options.getUfsAddress());
  }

  /**
   * Constructs a wrapper file system based on given UnderFileSystem. Caller should close the
   * FileSystemContext.
   *
   * @param fsContext file system context
   * @param options the ufs file system options
   * @param ufs the under file system
   */
  public UfsBaseFileSystem(FileSystemContext fsContext, UfsFileSystemOptions options,
      UfsManager.UfsClient ufs) {
    Preconditions.checkNotNull(fsContext);
    Preconditions.checkNotNull(options);
    mFsContext = fsContext;
    String ufsAddress = options.getUfsAddress();
    Preconditions.checkArgument(!ufsAddress.isEmpty(), "ufs address should not be empty");
    mRootUFS = new AlluxioURI(ufsAddress);
    mUfs = ufs.acquireUfsResource();
    mCloser.register(mUfs);
    LOG.debug("Creating file system connecting to ufs address {}", ufsAddress);
  }

  /**
   * Shuts down the FileSystem. Closes all thread pools and resources used to perform operations. If
   * any operations are called after closing the context the behavior is undefined.
   */
  @Override
  public synchronized void close() throws IOException {
    if (!mClosed) {
      mClosed = true;
    }
    mCloser.close();
  }

  @Override
  public synchronized boolean isClosed() {
    return mClosed;
  }

  @Override
  public void checkAccess(AlluxioURI path, CheckAccessPOptions options) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options) {
    call(() -> {
      // TODO(lu) deal with other options e.g. owner/group
      MkdirsOptions ufsOptions = MkdirsOptions.defaults(mFsContext.getClusterConf());
      if (options.hasMode()) {
        ufsOptions.setMode(Mode.fromProto(options.getMode()));
      }
      if (options.hasRecursive()) {
        ufsOptions.setCreateParent(options.getRecursive());
      }
      mUfs.get().mkdirs(path.toString(), ufsOptions);
    });
  }

  @Override
  public FileOutStream createFile(AlluxioURI path, CreateFilePOptions options) {
    return callWithReturn(() -> {
      // TODO(lu) deal with other options e.g. owner/group/acl/ensureAtomic
      CreateOptions ufsOptions = CreateOptions.defaults(mFsContext.getClusterConf());
      if (options.hasMode()) {
        ufsOptions.setMode(Mode.fromProto(options.getMode()));
      }
      if (options.hasRecursive()) {
        ufsOptions.setCreateParent(options.getRecursive());
      }
      if (options.hasIsAtomicWrite()) {
        ufsOptions.setEnsureAtomic(options.getIsAtomicWrite());
      }
      if (options.hasUseMultipartUpload()) {
        ufsOptions.setMultipartUploadEnabled(options.getUseMultipartUpload());
      }
      return new UfsFileOutStream(mUfs.get().create(path.getPath(), ufsOptions));
    });
  }

  @Override
  public void delete(AlluxioURI path, DeletePOptions options) {
    call(() -> {
      String ufsPath = path.toString();
      if (mUfs.get().isFile(ufsPath)) {
        mUfs.get().deleteFile(ufsPath);
        return;
      }
      DeleteOptions ufsOptions = DeleteOptions.defaults();
      if (options.hasRecursive()) {
        ufsOptions.setRecursive(options.getRecursive());
      }
      mUfs.get().deleteDirectory(ufsPath, ufsOptions);
    });
  }

  @Override
  public boolean exists(AlluxioURI path, final ExistsPOptions options) {
    return Boolean.TRUE.equals(callWithReturn(() -> mUfs.get().exists(path.toString())));
  }

  @Override
  public void free(AlluxioURI path, final FreePOptions options) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<BlockLocationInfo> getBlockLocations(AlluxioURI path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<BlockLocationInfo> getBlockLocations(URIStatus status) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AlluxioConfiguration getConf() {
    return mFsContext.getClusterConf();
  }

  @Override
  public URIStatus getStatus(AlluxioURI path) throws FileDoesNotExistException {
    return getStatus(path, GetStatusPOptions.getDefaultInstance());
  }

  @Override
  public URIStatus getStatus(AlluxioURI path, final GetStatusPOptions options)
      throws FileDoesNotExistException {
    return callWithReturn(() -> {
      UfsStatus ufsStatus = mUfs.get().getStatus(path.toString(), GetStatusOptions.defaults()
                            .setIncludeRealContentHash(options.getIncludeRealContentHash()));
      return transformStatus(ufsStatus, path.toString());
    });
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path, final ListStatusPOptions options) {
    return callWithReturn(() -> {
      ListOptions ufsOptions = ListOptions.defaults();
      if (options.hasRecursive()) {
        ufsOptions.setRecursive(options.getRecursive());
      }
      UfsStatus[] ufsStatuses = mUfs.get().listStatus(path.toString(), ufsOptions);
      if (ufsStatuses == null || ufsStatuses.length == 0) {
        return Collections.emptyList();
      }
      List<URIStatus> uriStatusList = new ArrayList<>();
      for (UfsStatus ufsStatus: ufsStatuses) {
        URIStatus uriStatus = transformStatus(ufsStatus,
            PathUtils.concatPath(path.toString(), ufsStatus.getName()));
        uriStatusList.add(uriStatus);
      }
      return uriStatusList;
    });
  }

  @Override
  public void iterateStatus(AlluxioURI path, final ListStatusPOptions options,
      Consumer<? super URIStatus> action) {
    call(() -> {
      ListOptions ufsOptions = ListOptions.defaults();
      if (options.hasRecursive()) {
        ufsOptions.setRecursive(options.getRecursive());
      }
      UfsStatus[] ufsStatuses = mUfs.get().listStatus(path.toString(), ufsOptions);
      if (ufsStatuses == null || ufsStatuses.length == 0) {
        return;
      }
      for (UfsStatus ufsStatus: ufsStatuses) {
        URIStatus uriStatus = transformStatus(ufsStatus,
            PathUtils.concatPath(path.toString(), ufsStatus.getName()));
        action.accept(uriStatus);
      }
    });
  }

  @Override
  public ListStatusPartialResult listStatusPartial(
      AlluxioURI path, final ListStatusPartialPOptions options) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void loadMetadata(AlluxioURI path, final ListStatusPOptions options) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, final MountPOptions options) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateMount(AlluxioURI alluxioPath, final MountPOptions options) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, MountPointInfo> getMountTable(boolean checkUfs) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void persist(final AlluxioURI path, final ScheduleAsyncPersistencePOptions options) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFilePOptions options)
      throws FileDoesNotExistException {
    return openFile(getStatus(path), options);
  }

  @Override
  public FileInStream openFile(URIStatus status, OpenFilePOptions options) {
    return callWithReturn(() -> {
      // TODO(lu) deal with other options e.g. maxUfsReadConcurrency
      return new UfsFileInStream(offset -> {
        try {
          return mUfs.get().open(status.getUfsPath(), OpenOptions.defaults().setOffset(offset));
        } catch (IOException e) {
          throw AlluxioRuntimeException.from(e);
        }
      }, status.getLength());
    });
  }

  @Override
  public PositionReader openPositionRead(AlluxioURI path, OpenFilePOptions options)
      throws FileDoesNotExistException {
    return openPositionRead(getStatus(path), options);
  }

  @Override
  public PositionReader openPositionRead(URIStatus status, OpenFilePOptions options) {
    return callWithReturn(() -> mUfs.get()
        .openPositionRead(status.getUfsPath(), status.getLength()));
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options) {
    call(() -> {
      String srcPath = src.toString();
      String dstPath = dst.toString();
      boolean renamed;
      if (mUfs.get().isFile(srcPath)) {
        renamed = mUfs.get().renameFile(srcPath, dstPath);
      } else {
        renamed = mUfs.get().renameDirectory(srcPath, dstPath);
      }
      if (!renamed) {
        throw new AlluxioRuntimeException(
            Status.FAILED_PRECONDITION,
            String.format("Failed to rename from %s to %s", srcPath, dstPath),
            null,
            ErrorType.External,
            false
        );
      }
    });
  }

  @Override
  public AlluxioURI reverseResolve(AlluxioURI ufsUri) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries,
      SetAclPOptions options) {
    call(() -> mUfs.get().setAclEntries(path.toString(), entries));
  }

  @Override
  public void setAttribute(AlluxioURI path, SetAttributePOptions options) {
    call(() -> {
      if (options.hasMode()) {
        mUfs.get().setMode(path.toString(), ModeUtils.protoToShort(options.getMode()));
      }
      if (options.hasOwner() && options.hasGroup()) {
        mUfs.get().setOwner(path.toString(), options.getOwner(), options.getGroup());
      } else if (options.hasOwner()) {
        mUfs.get().setOwner(path.toString(), options.getOwner(), null);
      } else if (options.hasGroup()) {
        mUfs.get().setOwner(path.toString(), null, options.getOwner());
      }
      if (options.hasPinned() || options.hasPersisted() || options.hasRecursive()
          || options.hasReplicationMax() || options.hasReplicationMin()
          || options.getXattrCount() != 0) {
        LOG.error("UFS only supports setting mode, owner, and group. Does not support setting {}",
            options);
        throw new UnsupportedOperationException(
            String.format("Cannot set attribute of %s", options));
      }
    });
  }

  @Override
  public void unmount(AlluxioURI path, UnmountPOptions options) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void needsSync(AlluxioURI path) throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<String> submitJob(JobRequest jobRequest) {
    try (CloseableResource<FileSystemMasterClient> client =
        mFsContext.acquireMasterClientResource()) {
      return client.get().submitJob(jobRequest);
    }
  }

  @Override
  public boolean stopJob(JobDescription jobDescription) {
    try (CloseableResource<FileSystemMasterClient> client =
        mFsContext.acquireMasterClientResource()) {
      return client.get().stopJob(jobDescription);
    }
  }

  @Override
  public String getJobProgress(JobDescription jobDescription,
      JobProgressReportFormat format, boolean verbose) {
    try (CloseableResource<FileSystemMasterClient> client =
        mFsContext.acquireMasterClientResource()) {
      return client.get().getJobProgress(jobDescription, format, verbose);
    }
  }

  /**
   * Transform UFS file/directory status to client-side status.
   *
   * @param ufsStatus the UFS status to transform
   * @return the client-side status
   */
  private URIStatus transformStatus(UfsStatus ufsStatus, String ufsFullPath) {
    AlluxioURI ufsUri = new AlluxioURI(ufsFullPath);
    String relativePath = CommonUtils.stripPrefixIfPresent(ufsFullPath, mRootUFS.toString());
    if (!relativePath.startsWith(AlluxioURI.SEPARATOR)) {
      relativePath = AlluxioURI.SEPARATOR + relativePath;
    }

    FileInfo info = new FileInfo().setName(ufsUri.getName())
        .setPath(relativePath)
        .setUfsPath(ufsFullPath)
        .setFileId(ufsUri.toString().hashCode())
        .setFolder(ufsStatus.isDirectory())
        .setOwner(ufsStatus.getOwner())
        .setGroup(ufsStatus.getGroup())
        .setMode(ufsStatus.getMode())
        .setCompleted(true);
    if (ufsStatus.getLastModifiedTime() != null) {
      info.setLastModificationTimeMs(ufsStatus.getLastModifiedTime());
    }
    if (ufsStatus.getXAttr() != null) {
      info.setXAttr(ufsStatus.getXAttr());
    }
    if (ufsStatus instanceof UfsFileStatus) {
      UfsFileStatus fileStatus = (UfsFileStatus) ufsStatus;
      info.setLength(fileStatus.getContentLength());
      info.setBlockSizeBytes(fileStatus.getBlockSize());
      info.setUfsFingerprint(
          Fingerprint.create(mUfs.get().getUnderFSType(), ufsStatus, fileStatus.getContentHash())
                     .serialize());
    }
    else {
      info.setLength(0);
    }
    return new URIStatus(info);
  }

  /**
   * Gets the UFS Root.
   *
   * @return AlluxioURI of UFS Root
   */
  public AlluxioURI getRootUFS() {
    return mRootUFS;
  }

  private static void call(UfsCallable callable) {
    try {
      callable.call();
    } catch (IOException e) {
      throw AlluxioRuntimeException.from(e);
    }
  }

  private static <T> T callWithReturn(UfsCallableWithReturn<T> callable) {
    try {
      return callable.call();
    } catch (IOException e) {
      throw AlluxioRuntimeException.from(e);
    }
  }

  interface UfsCallable {
    void call() throws IOException;
  }

  interface UfsCallableWithReturn<V> {
    V call() throws IOException;
  }
}
