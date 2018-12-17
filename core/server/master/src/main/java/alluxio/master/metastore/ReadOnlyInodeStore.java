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

import alluxio.master.file.meta.InodeView;

import java.util.Optional;

/**
 * Read-only access to the inode store.
 */
public interface ReadOnlyInodeStore {
  /**
   * @return the number of inodes in the inode store
   */
  int size();

  /**
   * @param id an inode id
   * @return the inode with the given id, if it exists
   */
  Optional<InodeView> get(long id);

  /**
   * @param parentId an inode id
   * @param name an inode name
   * @return whether the inode with the given id has a child with the given name
   */
  boolean hasChild(long parentId, String name);

  /**
   * @param id an inode id
   * @return an iterable over the children of the inode with the given id
   */
  Iterable<? extends InodeView> getChildren(long id);

  /**
   * @param parentId an inode id
   * @param name an inode name
   * @return the child of the parent with the given name
   */
  Optional<InodeView> getChild(long parentId, String name);

  /**
   * @param id an inode id
   * @return whether the inode has any children
   */
  boolean hasChildren(long id);
}
