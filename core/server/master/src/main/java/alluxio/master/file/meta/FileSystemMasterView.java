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

package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.file.FileSystemMaster;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class exposes a read-only view of {@link FileSystemMaster}.
 */
@ThreadSafe
public final class FileSystemMasterView {
  private final FileSystemMaster mFileSystemMaster;

  /**
   * Constructs a view of the {@link FileSystemMaster}.
   *
   * @param fileSystemMaster the file system master
   */
  public FileSystemMasterView(FileSystemMaster fileSystemMaster) {
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster, "fileSystemMaster");
  }

  /**
   * Returns the persistence state of a given file.
   *
   * @param fileId the file id
   * @return the persistence state
   * @throws FileDoesNotExistException if the file does not exist
   */
  public synchronized PersistenceState getFilePersistenceState(long fileId)
      throws FileDoesNotExistException {
    return mFileSystemMaster.getPersistenceState(fileId);
  }

  /**
   * Returns the {@link FileInfo} for a given path. Called via RPC, as well as internal masters.
   *
   * @param fileId the file id to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission denied
   */
  public synchronized FileInfo getFileInfo(long fileId)
      throws FileDoesNotExistException, AccessControlException, UnavailableException {
    return mFileSystemMaster.getFileInfo(fileId);
  }

  /**
   * @return all the files lost on the workers
   */
  public synchronized List<Long> getLostFiles() {
    return mFileSystemMaster.getLostFiles();
  }

  /**
   * Returns the file id for a given path. If the given path does not exist in Alluxio, the method
   * attempts to load it from UFS.
   *
   * @param path the path to get the file id for
   * @return the file id for a given path, or -1 if there is no file at that path
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if file does not exist
   */
  public synchronized long getFileId(AlluxioURI path)
      throws AccessControlException, FileDoesNotExistException, UnavailableException {
    return mFileSystemMaster.getFileId(path);
  }

  /**
   * @param path the path to get the info for
   * @return a list of {@link FileBlockInfo} for all the blocks of the given file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the path of the given file is invalid
   * @throws AccessControlException if permission checking fails
   */
  public synchronized List<FileBlockInfo> getFileBlockInfoList(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnavailableException {
    return mFileSystemMaster.getFileBlockInfoList(path);
  }

  /**
   * Gets the path of a file with the given id.
   *
   * @param fileId the id of the file to look up
   * @return the path of the file
   * @throws FileDoesNotExistException raise if the file does not exist
   */
  public synchronized AlluxioURI getPath(long fileId) throws FileDoesNotExistException {
    return mFileSystemMaster.getPath(fileId);
  }

  /**
   * @return a list of {@link WorkerInfo} objects representing the workers in Alluxio
   */
  public synchronized List<WorkerInfo> getWorkerInfoList() throws UnavailableException {
    return mFileSystemMaster.getWorkerInfoList();
  }
}
