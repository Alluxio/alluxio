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
import alluxio.exception.FileDoesNotExistException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a locked inode and its URI.
 */
@ThreadSafe
public interface LockedInode {

  /**
   * @return the full uri of the inode
   */
  AlluxioURI getUri();

  /**
   * @return the {@link Inode}
   * @throws FileDoesNotExistException if the inode does not exist
   */
  Inode<?> getInode() throws FileDoesNotExistException;

  /**
   * @return the {@link Inode}, or null if it does not exist
   */
  @Nullable
  Inode<?> getInodeOrNull();

  /**
   * @return the inode as an {@link InodeFile}
   * @throws FileDoesNotExistException if the inode does not exist, or is not an {@link InodeFile}
   */
  InodeFile getInodeFile() throws FileDoesNotExistException;
}
