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
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class represents a locked inode and its URI.
 */
@NotThreadSafe
public class TempLockedInode implements LockedInode {
  private AlluxioURI mUri;
  private Inode<?> mInode;

  /**
   * Constructs a new {@link TempLockedInode}.
   *
   * @param uri the {@link AlluxioURI} of the inode
   * @param inode the inode
   */
  public TempLockedInode(AlluxioURI uri, Inode<?> inode) {
    mUri = uri;
    mInode = inode;
  }

  @Override
  public AlluxioURI getUri() {
    return mUri;
  }

  @Override
  public Inode<?> getInode() throws FileDoesNotExistException {
    return mInode;
  }

  @Override
  @Nullable
  public Inode<?> getInodeOrNull() {
    return mInode;
  }

  @Override
  public InodeFile getInodeFile() throws FileDoesNotExistException {
    if (!mInode.isFile()) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(mUri));
    }
    return (InodeFile) mInode;
  }

  /**
   * @param uri the {@link AlluxioURI} of the inode
   * @param inode the inode
   */
  public void setInode(AlluxioURI uri, Inode<?> inode) {
    mUri = uri;
    mInode = inode;
  }
}
