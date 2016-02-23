/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.meta;

import alluxio.exception.FileDoesNotExistException;
import alluxio.master.file.FileSystemMaster;
import alluxio.wire.FileInfo;

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
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
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
   */
  public synchronized FileInfo getFileInfo(long fileId) throws FileDoesNotExistException {
    return mFileSystemMaster.getFileInfo(fileId);
  }

  /**
   * @return all the files lost on the workers
   */
  public synchronized List<Long> getLostFiles() {
    return mFileSystemMaster.getLostFiles();
  }
}
