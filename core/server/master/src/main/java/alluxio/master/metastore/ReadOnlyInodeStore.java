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

import java.util.Iterator;
import java.util.NoSuchElementException;
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
   * @param inodeId an inode id to list child ids for
   * @return the child ids iterable
   */
  Iterable<Long> getChildIds(Long inodeId);

  /**
   * Returns an iterable for the ids of the children of the given directory.
   *
   * @param inode the inode to list child ids for
   * @return the child ids iterable
   */
  default Iterable<Long> getChildIds(InodeDirectoryView inode) {
    return getChildIds(inode.getId());
  }

  /**
   * @param inodeId an inode id
   * @return an iterable over the children of the inode with the given id
   */
  default Iterable<? extends Inode> getChildren(Long inodeId) {
    return () -> {
      Iterator<Long> it = getChildIds(inodeId).iterator();
      return new Iterator<Inode>() {
        private Inode mNext = null;

        @Override
        public boolean hasNext() {
          advance();
          return mNext != null;
        }

        @Override
        public Inode next() {
          if (!hasNext()) {
            throw new NoSuchElementException("No more inodes");
          }
          Inode next = mNext;
          mNext = null;
          return next;
        }

        void advance() {
          while (mNext == null && it.hasNext()) {
            Long nextId = it.next();
            // Make sure the inode metadata still exists
            Optional<Inode> nextInode = get(nextId);
            if (nextInode.isPresent()) {
              mNext = nextInode.get();
            }
          }
        }
      };
    };
  }

  /**
   * @param inode an inode directory
   * @return an iterable over the children of the inode with the given id
   */
  default Iterable<? extends Inode> getChildren(InodeDirectoryView inode) {
    return getChildren(inode.getId());
  }

  /**
   * @param inodeId an inode id
   * @param name an inode name
   * @return the id of the child of the inode with the given name
   */
  Optional<Long> getChildId(Long inodeId, String name);

  /**
   * @param inode an inode directory
   * @param name an inode name
   * @return the id of the child of the inode with the given name
   */
  default Optional<Long> getChildId(InodeDirectoryView inode, String name) {
    return getChildId(inode.getId(), name);
  }

  /**
   * @param inodeId an inode id
   * @param name an inode name
   * @return the child of the inode with the given name
   */
  Optional<Inode> getChild(Long inodeId, String name);

  /**
   * @param inode an inode directory
   * @param name an inode name
   * @return the child of the inode with the given name
   */
  default Optional<Inode> getChild(InodeDirectoryView inode, String name) {
    return getChild(inode.getId(), name);
  }

  /**
   * @param inode an inode directory
   * @return whether the inode has any children
   */
  boolean hasChildren(InodeDirectoryView inode);
}
