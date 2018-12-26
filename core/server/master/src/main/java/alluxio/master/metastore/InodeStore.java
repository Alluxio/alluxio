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

import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeView;

/**
 * Inode metadata storage.
 *
 * The inode store manages metadata about individual inodes, as well as the parent-child
 * relationships between them.
 */
public interface InodeStore extends ReadOnlyInodeStore {
  /**
   * Removes an inode from the inode store. The edge leading to it will also be removed.
   *
   * @param inode an inode to remove
   */
  void remove(InodeView inode);

  /**
   * @param id an inode id
   * @param opTimeMs the last modification time to set
   */
  void setModificationTimeMs(long id, long opTimeMs);

  /**
   * Adds the given inode, or overwrites it if it exists.
   *
   * @param inode the inode to write
   */
  void writeInode(Inode<?> inode);

  /**
   * Removes all inodes and edges.
   */
  void clear();

  /**
   * Makes an inode the child of the specified parent.
   *
   * @param parentId the parent id
   * @param inode the child inode
   */
  void addChild(long parentId, InodeView inode);

  /**
   * Removes a child from a parent inode.
   *
   * @param parentId the parent inode id
   * @param name the child name
   */
  void removeChild(long parentId, String name);
}
