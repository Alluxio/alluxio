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

package alluxio.master.metastore;

import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.Inode;

import java.util.Optional;

/**
 * Read-only access to the inode store.
 */
public interface ReadOnlyInodeStore {
  /**
   * @return an estimate for the number of inodes in the inode store
   */
  long estimateSize();

  /**
   * @param id an inode id
   * @return the inode with the given id, if it exists
   */
  Optional<Inode> get(long id);

  /**
   * Returns an iterable for the ids of the children of the given directory.
   *
   * @param inode the inode to list child ids for
   * @return the child ids iterable
   */
  Iterable<Long> getChildIds(InodeDirectoryView inode);

  /**
   * @param inode an inode directory
   * @return an iterable over the children of the inode with the given id
   */
  Iterable<? extends Inode> getChildren(InodeDirectoryView inode);

  /**
   * @param inode an inode directory
   * @param name an inode name
   * @return the id of the child of the inode with the given name
   */
  Optional<Long> getChildId(InodeDirectoryView inode, String name);

  /**
   * @param inode an inode directory
   * @param name an inode name
   * @return the child of the inode with the given name
   */
  Optional<Inode> getChild(InodeDirectoryView inode, String name);

  /**
   * @param inode an inode directory
   * @return whether the inode has any children
   */
  boolean hasChildren(InodeDirectoryView inode);
}
