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

import alluxio.AlluxioURI;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CheckAccessPOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
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
import alluxio.security.authorization.AclEntry;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.ModeUtils;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SyncPointInfo;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
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
  protected final UnderFileSystem mUfs;
  protected volatile boolean mClosed = false;

  /**
   * Constructs a new base file system.
   *
   * @param fsContext file system context
   */
  public UfsBaseFileSystem(FileSystemContext fsContext) {
    mFsContext = fsContext;
    String ufsAddress = mFsContext.getClusterConf().getString(PropertyKey.USER_UFS_ADDRESS);
    Preconditions.checkArgument(!ufsAddress.isEmpty(), "ufs address should not be empty");
    mUfs = UnderFileSystem.Factory.create(ufsAddress, mFsContext.getClusterConf());
    mCloser.register(mFsContext);
    mCloser.register(mUfs);
    LOG.debug("Creating file system connecting to ufs address {}", ufsAddress);
  }

  /**
   * Shuts down the FileSystem. Closes all thread pools and resources used to perform operations. If
   * any operations are called after closing the context the behavior is undefined.
   */
  @Override
  public synchronized void close() throws IOException {
    // TODO(zac) Determine the behavior when closing the context during operations.
    if (!mClosed) {
      mClosed = true;
    }
    mCloser.close();
  }

  @Override
  public boolean isClosed() {
    // Doesn't require locking because mClosed is volatile and marked first upon close
    return mClosed;
  }

  @Override
  public void checkAccess(AlluxioURI path, CheckAccessPOptions options) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options) throws IOException {
    mUfs.mkdirs(path.getPath());
  }

  @Override
  public FileOutStream createFile(AlluxioURI path, CreateFilePOptions options) throws IOException {
    return new UfsFileOutStream(mUfs.create(path.getPath()));
  }

  @Override
  public void delete(AlluxioURI path, DeletePOptions options) throws IOException {
    String ufsPath = path.getPath();
    if (mUfs.isFile(ufsPath)) {
      mUfs.deleteFile(ufsPath);
    } else {
      mUfs.deleteDirectory(ufsPath);
    }
  }

  @Override
  public boolean exists(AlluxioURI path, final ExistsPOptions options) throws IOException {
    return mUfs.exists(path.getPath());
  }

  @Override
  public void free(AlluxioURI path, final FreePOptions options) {
    // TODO(lu) implement?
    throw new UnsupportedOperationException();
  }

  @Override
  public List<BlockLocationInfo> getBlockLocations(AlluxioURI path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<BlockLocationInfo> getBlockLocations(URIStatus status, AlluxioURI path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AlluxioConfiguration getConf() {
    return mFsContext.getClusterConf();
  }

  @Override
  public URIStatus getStatus(AlluxioURI path, final GetStatusPOptions options) throws IOException {
    String ufsPath = path.getPath();
    // TODO(lu) how to deal with completed since ufs status does not contain this info?
    return transformStatus(mUfs.isFile(ufsPath)
        ? mUfs.getFileStatus(ufsPath) : mUfs.getDirectoryStatus(ufsPath));
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path, final ListStatusPOptions options)
      throws IOException {
    UfsStatus[] ufsStatuses = mUfs.listStatus(path.getPath());
    if (ufsStatuses == null || ufsStatuses.length == 0) {
      return Collections.emptyList();
    }
    return Arrays.stream(ufsStatuses).map(this::transformStatus).collect(Collectors.toList());
  }

  @Override
  public void iterateStatus(AlluxioURI path, final ListStatusPOptions options,
      Consumer<? super URIStatus> action) throws IOException {
    UfsStatus[] ufsStatuses = mUfs.listStatus(path.getPath());
    if (ufsStatuses == null || ufsStatuses.length == 0) {
      return;
    }
    Arrays.stream(ufsStatuses).map(this::transformStatus).forEach(action);
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
  public List<SyncPointInfo> getSyncPathList() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void persist(final AlluxioURI path, final ScheduleAsyncPersistencePOptions options) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFilePOptions options) throws IOException {
    return new UfsFileInStream(mUfs.open(path.getPath()),
        mUfs.getFileStatus(path.getPath()).getContentLength());
  }

  @Override
  public FileInStream openFile(URIStatus status, OpenFilePOptions options) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options) throws IOException {
    // TODO(lu) rename logics
    String srcPath = src.getPath();
    String dstPath = dst.getPath();
    if (mUfs.isFile(srcPath)) {
      mUfs.renameFile(srcPath, dstPath);
    } else {
      mUfs.renameDirectory(srcPath, dstPath);
    }
  }

  @Override
  public AlluxioURI reverseResolve(AlluxioURI ufsUri) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries,
      SetAclPOptions options) throws IOException {
    mUfs.setAclEntries(path.getPath(), entries);
  }

  @Override
  public void setAttribute(AlluxioURI path, SetAttributePOptions options) throws IOException {
    if (options.hasMode()) {
      mUfs.setMode(path.getPath(), ModeUtils.protoToShort(options.getMode()));
    }
    if (options.hasOwner() && options.hasGroup()) {
      // TODO(lu) see if owner or group getOwner return null or error out?
      mUfs.setOwner(path.getPath(), options.getOwner(), options.getGroup());
    } else if (options.hasOwner()) {
      mUfs.setOwner(path.getPath(), options.getOwner(), null);
    } else if (options.hasGroup()) {
      mUfs.setOwner(path.getPath(), null, options.getOwner());
    }
    if (options.hasPinned() || options.hasPersisted() || options.hasRecursive()
        || options.hasReplicationMax() || options.hasReplicationMin()
        || options.getXattrCount() != 0) {
      LOG.error("UFS only supports setting mode, owner, and group. Does not support setting {}",
          options);
      throw new UnsupportedOperationException(String.format("Cannot set attribute of %s", options));
    }
  }

  /**
   * Starts the active syncing process on an Alluxio path.
   *
   * @param path the path to sync
   */
  @Override
  public void startSync(AlluxioURI path) {
    throw new UnsupportedOperationException();
  }

  /**
   * Stops the active syncing process on an Alluxio path.
   * @param path the path to stop syncing
   */
  @Override
  public void stopSync(AlluxioURI path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unmount(AlluxioURI path, UnmountPOptions options) {
    throw new UnsupportedOperationException();
  }

  private URIStatus transformStatus(UfsStatus ufsStatus) {
    FileInfo info = new FileInfo().setName(ufsStatus.getName())
        .setFolder(ufsStatus.isDirectory())
        .setOwner(ufsStatus.getOwner()).setGroup(ufsStatus.getGroup())
        .setMode(ufsStatus.getMode()).setXAttr(ufsStatus.getXAttr())
        .setCompleted(true);
    if (ufsStatus.getLastModifiedTime() != null) {
      info.setLastModificationTimeMs(info.getLastModificationTimeMs());
    }
    if (ufsStatus instanceof UfsFileStatus) {
      UfsFileStatus fileStatus = (UfsFileStatus) ufsStatus;
      info.setLength(fileStatus.getContentLength());
      info.setBlockSizeBytes(fileStatus.getBlockSize());
    } else {
      info.setLength(0);
    }
    return new URIStatus(info);
  }
}
