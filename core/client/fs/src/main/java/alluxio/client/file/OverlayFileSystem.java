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
import alluxio.client.file.options.FileSystemOptions;
import alluxio.client.file.ufs.UfsBaseFileSystem;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.FileIncompleteException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.OpenDirectoryException;
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
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SyncPointInfo;

import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class OverlayFileSystem implements FileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(OverlayFileSystem.class);
  public static final int DUMMY_MOUNT_ID = 0;

  private final DoraCacheFileSystem mDoraCacheFileSystem;
  private final FileSystem mWriteableFileSystem;

  public OverlayFileSystem(FileSystem fs, FileSystemContext context, FileSystemOptions options) {
    mDoraCacheFileSystem = new DoraCacheFileSystem(
        new UfsBaseFileSystem(context, options.getUfsFileSystemOptions().get()),
        context);
    mWriteableFileSystem = new BaseFileSystem(context);
  }

  @Override
  public boolean isClosed() {
    return mDoraCacheFileSystem.isClosed() && mWriteableFileSystem.isClosed();
  }

  @Override
  public void checkAccess(AlluxioURI path, CheckAccessPOptions options)
      throws InvalidPathException, IOException, AlluxioException {
    try {
      mDoraCacheFileSystem.checkAccess(path, options);
    } catch (InvalidPathException e) {
      mWriteableFileSystem.checkAccess(path,options);
    }
  }

  @Override
  public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    mWriteableFileSystem.createDirectory(path, options);
  }

  @Override
  public FileOutStream createFile(AlluxioURI path, CreateFilePOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    return mWriteableFileSystem.createFile(path, options);
  }

  @Override
  public void delete(AlluxioURI path, DeletePOptions options)
      throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException {
    mWriteableFileSystem.delete(path, options);
    mDoraCacheFileSystem.delete(path, options);
  }

  @Override
  public boolean exists(AlluxioURI path, ExistsPOptions options)
      throws InvalidPathException, IOException, AlluxioException {
    return mDoraCacheFileSystem.exists(path, options)
        || mWriteableFileSystem.exists(path, options);
  }

  @Override
  public void free(AlluxioURI path, FreePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    mDoraCacheFileSystem.free(path, options);
  }

  @Override
  public List<BlockLocationInfo> getBlockLocations(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    throw new NotImplementedException("");
    // return mDelegatedFileSystem.getBlockLocations(path);
  }

  @Override
  public List<BlockLocationInfo> getBlockLocations(URIStatus status)
      throws FileDoesNotExistException, IOException, AlluxioException {
    throw new NotImplementedException("");
    // return mDelegatedFileSystem.getBlockLocations(status);
  }

  @Override
  public AlluxioConfiguration getConf() {
    return mWriteableFileSystem.getConf();
  }

  @Override
  public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    try {
      return mDoraCacheFileSystem.getStatus(path, options);
    } catch (InvalidPathException e) {
      return mWriteableFileSystem.getStatus(path, options);
    }
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    // TODO(yyong) fix me
    List<URIStatus> result1 = mWriteableFileSystem.listStatus(path, options);
    List<URIStatus> result2 = mDoraCacheFileSystem.listStatus(path, options);
    if (result1.addAll(result2)) {
      return result1;
    }
    throw new AlluxioException("Failed to merge two lists");
  }

  @Override
  public ListStatusPartialResult listStatusPartial(
      AlluxioURI path, ListStatusPartialPOptions options)
      throws AlluxioException, IOException {
    // TODO(yyong) fix me
    throw new NotImplementedException();
    // mDelegatedFileSystem.listStatusPartial(path, options);
  }

  @Override
  public void iterateStatus(AlluxioURI path, ListStatusPOptions options,
      Consumer<? super URIStatus> action)
      throws FileDoesNotExistException, IOException, AlluxioException {
    // TODO(yyong) fix me
    throw new NotImplementedException();
    // mDelegatedFileSystem.iterateStatus(path, options, action);
  }

  @Override
  public void loadMetadata(AlluxioURI path, ListStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    mDoraCacheFileSystem.loadMetadata(path, options);
  }

  @Override
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountPOptions options)
      throws IOException, AlluxioException {
    mWriteableFileSystem.mount(alluxioPath, ufsPath, options);
  }

  @Override
  public void updateMount(AlluxioURI alluxioPath, MountPOptions options)
      throws IOException, AlluxioException {
    mWriteableFileSystem.updateMount(alluxioPath, options);
  }

  @Override
  public Map<String, MountPointInfo> getMountTable(boolean checkUfs)
      throws IOException, AlluxioException {
    return mWriteableFileSystem.getMountTable(checkUfs);
  }

  @Override
  public List<SyncPointInfo> getSyncPathList() throws IOException, AlluxioException {
    return mWriteableFileSystem.getSyncPathList();
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFilePOptions options)
      throws FileDoesNotExistException, OpenDirectoryException, FileIncompleteException,
      IOException, AlluxioException {
    try {
      return mDoraCacheFileSystem.openFile(path, options);
    } catch (FileDoesNotExistException e) {
      return mWriteableFileSystem.openFile(path, options);
    }
  }

  @Override
  public FileInStream openFile(URIStatus status, OpenFilePOptions options)
      throws FileDoesNotExistException, OpenDirectoryException, FileIncompleteException,
      IOException, AlluxioException {
    try {
      return mDoraCacheFileSystem.openFile(status, options);
    } catch (FileDoesNotExistException e) {
      return mWriteableFileSystem.openFile(status, options);
    }
  }

  @Override
  public void persist(AlluxioURI path, ScheduleAsyncPersistencePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    mWriteableFileSystem.persist(path, options);
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    mWriteableFileSystem.rename(src, dst, options);
  }

  @Override
  public AlluxioURI reverseResolve(AlluxioURI ufsUri) throws IOException, AlluxioException {
    return mWriteableFileSystem.reverseResolve(ufsUri);
  }

  @Override
  public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries,
      SetAclPOptions options) throws FileDoesNotExistException, IOException, AlluxioException {
    try {
      mDoraCacheFileSystem.setAcl(path, action, entries, options);
    } catch (FileDoesNotExistException e) {
      mWriteableFileSystem.setAcl(path, action, entries, options);
    }
  }

  @Override
  public void startSync(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    mWriteableFileSystem.startSync(path);
  }

  @Override
  public void stopSync(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    mWriteableFileSystem.stopSync(path);
  }

  @Override
  public void setAttribute(AlluxioURI path, SetAttributePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    try {
      mDoraCacheFileSystem.setAttribute(path, options);
    } catch (FileDoesNotExistException e) {
      mWriteableFileSystem.setAttribute(path, options);
    }
  }

  @Override
  public void unmount(AlluxioURI path, UnmountPOptions options)
      throws IOException, AlluxioException {
    mWriteableFileSystem.unmount(path, options);
  }

  @Override
  public void needsSync(AlluxioURI path) throws IOException, AlluxioException {
    mWriteableFileSystem.needsSync(path);
  }

  @Override
  public boolean submitLoad(AlluxioURI path, java.util.OptionalLong bandwidth,
      boolean usePartialListing, boolean verify) {
    throw new NotImplementedException("Not implemented yet");
    // mDelegatedFileSystem.submitLoad(path, bandwidth, usePartialListing, verify);
  }

  @Override
  public boolean stopLoad(AlluxioURI path) {
    throw new NotImplementedException("Not implemented yet");
    // mDelegatedFileSystem.stopLoad(path);
  }

  @Override
  public String getLoadProgress(AlluxioURI path,
      java.util.Optional<alluxio.grpc.LoadProgressReportFormat> format, boolean verbose) {
    throw new NotImplementedException("Not implemented yet");
    // mDelegatedFileSystem.getLoadProgress(path, format, verbose);
  }

  @Override
  public void close() throws IOException {
    mWriteableFileSystem.close();
    mDoraCacheFileSystem.close();
  }
}
