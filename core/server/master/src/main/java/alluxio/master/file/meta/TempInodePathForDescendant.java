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
import alluxio.exception.InvalidPathException;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a temporary {@link LockedInodePath}. This {@link LockedInodePath} will
 * not unlock the inodes on close. This {@link LockedInodePath} can set any descendant (does not
 * have to be immediate).
 *
 * This is useful for being able to pass in descendant inodes to methods which require a
 * {@link LockedInodePath}, without having to re-traverse the inode tree, and re-acquire locks.
 * This allows methods to operate on descendants associated with an existing
 * {@link LockedInodePath}.
 */
// TODO(gpang): can an iterator for a LockedInodePath handle functionality for this class?
@ThreadSafe
public final class TempInodePathForDescendant extends LockedInodePath {
  private AlluxioURI mDescendantUri;
  private Inode<?> mDescendantInode;

  /**
   * Constructs a temporary {@link LockedInodePath} from an existing {@link LockedInodePath}.
   *
   * @param inodePath the {@link LockedInodePath} to create the temporary path from
   */
  public TempInodePathForDescendant(LockedInodePath inodePath) {
    super(inodePath);
    mDescendantUri = new AlluxioURI(inodePath.mUri.toString());
    mDescendantInode = null;
  }

  /**
   * Sets the already locked descendant inode for this temporary {@link LockedInodePath}.
   *
   * @param descendantInode the descendant inode, which should already be locked
   * @param uri the path of this descendant
   */
  public synchronized void setDescendant(Inode<?> descendantInode, AlluxioURI uri) {
    mDescendantInode = descendantInode;
    mDescendantUri = uri;
  }

  @Override
  public synchronized AlluxioURI getUri() {
    if (mDescendantInode == null) {
      return super.getUri();
    }
    return mDescendantUri;
  }

  @Override
  public synchronized Inode<?> getInode() throws FileDoesNotExistException {
    if (mDescendantInode == null) {
      return super.getInode();
    }
    return mDescendantInode;
  }

  @Override
  public synchronized  InodeDirectory getParentInodeDirectory()
      throws InvalidPathException, FileDoesNotExistException {
    if (mDescendantInode == null) {
      return super.getParentInodeDirectory();
    }
    try {
      if (super.getInode().getId() == mDescendantInode.getParentId()) {
        // It is possible to get the descendant parent if the descendant is the direct child.
        Inode<?> parentInode = super.getInode();
        if (!parentInode.isDirectory()) {
          throw new InvalidPathException(
              ExceptionMessage.PATH_MUST_HAVE_VALID_PARENT.getMessage(mDescendantUri));
        }
        return (InodeDirectory) parentInode;
      } else {
        throw new UnsupportedOperationException();
      }
    } catch (FileDoesNotExistException e) {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public synchronized List<Inode<?>> getInodeList() {
    if (mDescendantInode == null) {
      return super.getInodeList();
    }
    try {
      if (super.getInode().getId() == mDescendantInode.getParentId()) {
        // Only return the inode list if the descendant is the direct child.
        List<Inode<?>> inodeList = super.getInodeList();
        inodeList.add(mDescendantInode);
        return inodeList;
      } else {
        throw new UnsupportedOperationException();
      }
    } catch (FileDoesNotExistException e) {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public synchronized boolean fullPathExists() {
    if (mDescendantInode == null) {
      return super.fullPathExists();
    }
    return true;
  }

  @Override
  public synchronized void close() {
    // nothing to close
  }
}
