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

import com.google.common.base.Preconditions;

import tachyon.exception.FileDoesNotExistException;

/**
 * This class exposes a readonly view of {@link InodeTree}. This class is thread-safe.
 */
public final class FileStoreView {
  private final InodeTree mInodeTree;

  /**
   * Constructs a view of the file store.
   *
   * @param mInodeTree the inode tree
   */
  public FileStoreView(InodeTree inodeTree) {
    mInodeTree = Preconditions.checkNotNull(inodeTree);
  }

  /**
   * Returns the persistence state of a given file.
   *
   * @param fileId the file id
   * @return the persistence state
   * @throws FileDoesNotExistException if the file does not exist
   */
  public FilePersistenceState getFilePersistenceState(long fileId)
      throws FileDoesNotExistException {
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);
      return inode.getPersistenceState();
    }
  }
}
