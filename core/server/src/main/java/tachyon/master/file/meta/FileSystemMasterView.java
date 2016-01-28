/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.file.meta;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;

import tachyon.exception.FileDoesNotExistException;
import tachyon.master.file.FileSystemMaster;
import tachyon.thrift.FileInfo;

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
