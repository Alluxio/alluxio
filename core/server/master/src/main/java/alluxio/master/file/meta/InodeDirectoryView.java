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

import alluxio.exception.InvalidPathException;

import java.util.Collection;

import javax.annotation.Nullable;

/**
 * Read-only interface for an inode directory.
 */
public interface InodeDirectoryView extends InodeView {

  /**
   * @param name the name of the child
   * @return the inode with the given name, or null if there is no child with that name
   */
  InodeView getChild(String name);

  /**
   * @param name the name of the child
   * @param lockList the lock list to add the lock to
   * @return the read-locked inode with the given name, or null if there is no such child
   * @throws InvalidPathException if the path to the child is invalid
   */
  @Nullable
  InodeView getChildReadLock(String name, InodeLockList lockList) throws
      InvalidPathException;

  /**
   * @param name the name of the child
   * @param lockList the lock list to add the lock to
   * @return the write-locked inode with the given name, or null if there is no such child
   * @throws InvalidPathException if the path to the child is invalid
   */
  @Nullable
  InodeView getChildWriteLock(String name, InodeLockList lockList) throws
      InvalidPathException;

  /**
   * @return an unmodifiable collection of the children inodes
   */
  Collection<InodeView> getChildren();

  /**
   * @return the ids of the children
   */
  Collection<Long> getChildrenIds();

  /**
   * @return the number of children in the directory
   */
  int getNumberOfChildren();

  /**
   * @return true if the inode is a mount point, false otherwise
   */
  boolean isMountPoint();

  /**
   * @return true if we have loaded all the direct children's metadata once
   */
  boolean isDirectChildrenLoaded();

  /**
   * Before calling this method, the caller should hold at least a READ LOCK on the inode.
   *
   * @return true if we have loaded all the direct and indirect children's metadata once
   */
  boolean areDescendantsLoaded();
}
