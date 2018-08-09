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

package alluxio.master.file.state;

import alluxio.collections.FieldIndex;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeView;

/**
 * Read-only access to the inodes index.
 */
public class InodesView {
  /** Use UniqueFieldIndex directly for ID index rather than using IndexedSet. */
  private final FieldIndex<Inode<?>, Long> mInodes;

  /**
   * @param inodes the inodes to create a view for
   */
  public InodesView(FieldIndex<Inode<?>, Long> inodes) {
    mInodes = inodes;
  }

  /**
   * Returns whether an inode exists with the specified ID.
   *
   * @param id the id
   * @return true if there is one such inode, otherwise false
   */
  public boolean containsId(long id) {
    return mInodes.containsField(id);
  }

  /**
   * Gets a view of the inode with the specified ID.
   *
   * @param id the id
   * @return the inode or null if there is no such inode
   */
  public InodeView getById(long id) {
    return mInodes.getFirst(id);
  }

  /**
   * @return the number of objects in this index set
   */
  public int size() {
    return mInodes.size();
  }

}
