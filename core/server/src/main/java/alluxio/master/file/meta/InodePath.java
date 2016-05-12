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

import alluxio.AlluxioURI;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a path of locked {@link Inode}, starting from the root.
 */
@ThreadSafe
public final class InodePath implements AutoCloseable {
  private final AlluxioURI mUri;
  private final ArrayList<Inode<?>> mInodes;
  private final InodeLockGroup mLockGroup;

  InodePath(AlluxioURI uri, List<Inode<?>> inodes, InodeLockGroup lockGroup) {
    Preconditions.checkArgument(!inodes.isEmpty());
    mUri = uri;
    mInodes = new ArrayList<>(inodes);
    mLockGroup = lockGroup;
  }

  public synchronized AlluxioURI getUri() {
    return mUri;
  }

  public synchronized Inode getInode() {
    return mInodes.get(mInodes.size() - 1);
  }

  public synchronized  InodeFile getInodeFile() throws FileDoesNotExistException {
    Inode inode = getInode();
    if (!inode.isFile()) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(mUri));
    }
    return (InodeFile) inode;
  }

  @Override
  public synchronized void close() {
    mLockGroup.unlock();
  }
}
