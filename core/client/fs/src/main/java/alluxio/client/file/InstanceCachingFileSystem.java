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
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.FileIncompleteException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.OpenDirectoryException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
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

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A wrapper of a FileSystem instance. This wrapper will remove itself from the cache
 * on close.
 */
public class InstanceCachingFileSystem implements FileSystem {
  private final FileSystem mFileSystem;
  private final FileSystemCache mCache;
  private final FileSystemCache.Key mKey;

  /**
   * Wraps a file system instance to cache
   *
   * @param fs file system context
   * @param FileSystemCache fs instance cache
   */
  InstanceCachingFileSystem(FileSystem fs, FileSystemCache cache, FileSystemCache.Key key) {
    mFileSystem = fs;
    mCache = cache;
    mKey = key;
  }

  @Override
  public boolean isClosed() {
    return mFileSystem.isClosed();
  }

  @Override
  public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options) throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    mFileSystem.createDirectory(path, options);
  }

  @Override
  public FileOutStream createFile(AlluxioURI path, CreateFilePOptions options) throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    return mFileSystem.createFile(path, options);
  }

  @Override
  public void delete(AlluxioURI path, DeletePOptions options) throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException {
    mFileSystem.delete(path, options);
  }

  @Override
  public boolean exists(AlluxioURI path, ExistsPOptions options) throws InvalidPathException, IOException, AlluxioException {
    return mFileSystem.exists(path, options);
  }

  @Override
  public void free(AlluxioURI path, FreePOptions options) throws FileDoesNotExistException, IOException, AlluxioException {
    mFileSystem.free(path, options);
  }

  @Override
  public List<BlockLocationInfo> getBlockLocations(AlluxioURI path) throws FileDoesNotExistException, IOException, AlluxioException {
    return mFileSystem.getBlockLocations(path);
  }

  @Override
  public AlluxioConfiguration getConf() {
    return mFileSystem.getConf();
  }

  @Override
  public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options) throws FileDoesNotExistException, IOException, AlluxioException {
    return mFileSystem.getStatus(path, options);
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options) throws FileDoesNotExistException, IOException, AlluxioException {
    return mFileSystem.listStatus(path, options);
  }

  @Override
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountPOptions options) throws IOException, AlluxioException {
    mFileSystem.mount(alluxioPath, ufsPath);
  }

  @Override
  public void updateMount(AlluxioURI alluxioPath, MountPOptions options) throws IOException, AlluxioException {
    mFileSystem.updateMount(alluxioPath, options);
  }

  @Override
  public Map<String, MountPointInfo> getMountTable() throws IOException, AlluxioException {
    return mFileSystem.getMountTable();
  }

  @Override
  public List<SyncPointInfo> getSyncPathList() throws IOException, AlluxioException {
    return mFileSystem.getSyncPathList();
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFilePOptions options) throws FileDoesNotExistException, OpenDirectoryException, FileIncompleteException, IOException, AlluxioException {
    return mFileSystem.openFile(path, options);
  }

  @Override
  public void persist(AlluxioURI path, ScheduleAsyncPersistencePOptions options) throws FileDoesNotExistException, IOException, AlluxioException {
    mFileSystem.persist(path, options);
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options) throws FileDoesNotExistException, IOException, AlluxioException {
    mFileSystem.rename(src, dst, options);
  }

  @Override
  public AlluxioURI reverseResolve(AlluxioURI ufsUri) throws IOException, AlluxioException {
    return mFileSystem.reverseResolve(ufsUri);
  }

  @Override
  public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries, SetAclPOptions options) throws FileDoesNotExistException, IOException, AlluxioException {
    mFileSystem.setAcl(path, action, entries);
  }

  @Override
  public void startSync(AlluxioURI path) throws FileDoesNotExistException, IOException, AlluxioException {
    mFileSystem.startSync(path);
  }

  @Override
  public void stopSync(AlluxioURI path) throws FileDoesNotExistException, IOException, AlluxioException {
    mFileSystem.stopSync(path);
  }

  @Override
  public void setAttribute(AlluxioURI path, SetAttributePOptions options) throws FileDoesNotExistException, IOException, AlluxioException {
    mFileSystem.setAttribute(path, options);
  }

  @Override
  public void unmount(AlluxioURI path, UnmountPOptions options) throws IOException, AlluxioException {
    mFileSystem.unmount(path, options);
  }

  @Override
  public void close() throws IOException {
    mFileSystem.close();
    mCache.remove(mKey);
  }
}
