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

package alluxio.fuse.auth;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
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
import alluxio.wire.FileInfo;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SyncPointInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * An implementation of the file system that supports set user group of file
 * and get the user group file information.
 */
public class UserGroupFileSystem implements FileSystem {
  // Set attribute and get back the set attribtue information

  private final Map<AlluxioURI, URIStatus> mFiles = new HashMap<>();

  @Override
  public boolean isClosed() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void checkAccess(AlluxioURI path, CheckAccessPOptions options)
      throws InvalidPathException, IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileOutStream createFile(AlluxioURI path, CreateFilePOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(AlluxioURI path, DeletePOptions options)
      throws DirectoryNotEmptyException, FileDoesNotExistException, IOException,
      AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean exists(AlluxioURI path, ExistsPOptions options)
      throws InvalidPathException, IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void free(AlluxioURI path, FreePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<BlockLocationInfo> getBlockLocations(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public AlluxioConfiguration getConf() {
    return Configuration.global();
  }

  @Override
  public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    if (mFiles.containsKey(path)) {
      return mFiles.get(path);
    } else {
      throw new FileDoesNotExistException(path);
    }
  }

  @Override
  public void iterateStatus(AlluxioURI path, ListStatusPOptions options,
                            Consumer<? super URIStatus> action)
      throws FileDoesNotExistException, IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void loadMetadata(AlluxioURI path, ListStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountPOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateMount(AlluxioURI alluxioPath, MountPOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, MountPointInfo> getMountTable()
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SyncPointInfo> getSyncPathList() throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFilePOptions options)
      throws FileDoesNotExistException, OpenDirectoryException, FileIncompleteException,
      IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileInStream openFile(URIStatus status, OpenFilePOptions options)
      throws FileDoesNotExistException, OpenDirectoryException, FileIncompleteException,
      IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void persist(AlluxioURI path, ScheduleAsyncPersistencePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public AlluxioURI reverseResolve(AlluxioURI ufsUri)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries,
      SetAclPOptions options) throws FileDoesNotExistException, IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void startSync(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void stopSync(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setAttribute(AlluxioURI path, SetAttributePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    FileInfo info = new FileInfo();
    info.setFileId(path.toString().hashCode());
    if (options.hasOwner()) {
      info.setOwner(options.getOwner());
    }
    if (options.hasGroup()) {
      info.setGroup(options.getGroup());
    }
    mFiles.put(path, new URIStatus(info));
  }

  @Override
  public void unmount(AlluxioURI path, UnmountPOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException();
  }
}
