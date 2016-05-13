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
import alluxio.exception.InvalidPathException;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a path of locked {@link Inode}, starting from the root.
 */
@ThreadSafe
public abstract class InodePath implements AutoCloseable {
  protected final AlluxioURI mUri;
  protected final String[] mPathComponents;
  protected final ArrayList<Inode<?>> mInodes;
  protected final InodeLockGroup mLockGroup;

  InodePath(AlluxioURI uri, List<Inode<?>> inodes, InodeLockGroup lockGroup)
      throws InvalidPathException {
    Preconditions.checkArgument(!inodes.isEmpty());
    mUri = uri;
    mPathComponents = PathUtils.getPathComponents(mUri.getPath());
    mInodes = new ArrayList<>(inodes);
    mLockGroup = lockGroup;
  }

  public synchronized AlluxioURI getUri() {
    return mUri;
  }

  public synchronized Inode getInode() throws FileDoesNotExistException {
    if (!fullPathExists()) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(mUri));
    }
    return mInodes.get(mInodes.size() - 1);
  }

  public synchronized  InodeFile getInodeFile() throws FileDoesNotExistException {
    Inode inode = getInode();
    if (!inode.isFile()) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(mUri));
    }
    return (InodeFile) inode;
  }

  public synchronized boolean fullPathExists() {
    return mInodes.size() == mPathComponents.length;
  }

  @Override
  public synchronized void close() {
    mLockGroup.unlock();
  }
}
