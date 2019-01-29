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

import alluxio.conf.AlluxioConfiguration;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.MutableInode;

import java.util.Optional;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Inode metadata storage.
 *
 * The inode store manages metadata about individual inodes, as well as the parent-child
 * relationships between them.
 *
 * Writes to the inode store happen atomically, so it is safe to concurrently read and write the
 * same key. It is undefined whether the read will see the old value or the new value.
 */
@ThreadSafe
public interface InodeStore extends ReadOnlyInodeStore {
  /**
   * @param id an inode id
   * @return the inode with the given id, if it exists
   */
  Optional<MutableInode<?>> getMutable(long id);

  @Override
  default Optional<Inode> get(long id) {
    return getMutable(id).map(inode -> Inode.wrap(inode));
  }

  /**
   * Removes an inode from the inode store. This does *not* remove the edge leading to the inode.
   *
   * @param inodeId an inode to remove
   */
  void remove(Long inodeId);

  /**
   * Removes an inode from the inode store. This does *not* remove the edge leading to the inode.
   *
   * @param inode an inode to remove
   */
  default void remove(InodeView inode) {
    remove(inode.getId());
  }

  /**
   * Removes an inode and the edge leading to it from the inode store.
   *
   * @param inode an inode to remove
   */
  default void removeInodeAndParentEdge(InodeView inode) {
    remove(inode);
    removeChild(inode.getParentId(), inode.getName());
  }

  /**
   * Adds the given inode, or overwrites it if it already exists.
   *
   * If it is known that the inode is new, prefer {@link #writeNewInode(MutableInode)}.
   *
   * @param inode the inode to write
   */
  void writeInode(MutableInode<?> inode);

  /**
   * Adds a new inode.
   *
   * This method is similar to {@link #writeInode(MutableInode)}, but with an added information that
   * the inode is new. This allows some inode stores to perform extra optimizations.
   *
   * @param inode the inode to write
   */
  default void writeNewInode(MutableInode<?> inode) {
    writeInode(inode);
  }

  /**
   * Creates a write batch. Not all implementations support this, so callers should first check
   * {@link #supportsBatchWrite()}.
   *
   * @return a {@link WriteBatch} which can be used to perform a batched write
   */
  default WriteBatch createWriteBatch() {
    throw new UnsupportedOperationException(
        "batch writes are not supported for " + getClass().getSimpleName());
  }

  /**
   * @return whether the inode store implementation supports batch writes
   */
  default boolean supportsBatchWrite() {
    return false;
  }

  /**
   * Removes all inodes and edges.
   */
  void clear();

  /**
   * Makes an inode the child of the specified parent.
   *
   * @param parentId the parent id
   * @param childName the child name
   * @param childId the child inode id
   */
  void addChild(long parentId, String childName, Long childId);

  /**
   * Makes an inode the child of the specified parent.
   *
   * @param parentId the parent id
   * @param child the child inode
   */
  default void addChild(long parentId, InodeView child) {
    addChild(parentId, child.getName(), child.getId());
  }

  /**
   * Removes a child from a parent inode.
   *
   * @param parentId the parent inode id
   * @param name the child name
   */
  void removeChild(long parentId, String name);

  /**
   * Used to perform batched writes. Call {@link #createWriteBatch()} to use batched writes.
   *
   * Write batches may or may not be applied atomically.
   */
  interface WriteBatch {
    /**
     * Adds an inode to the write batch. This method serializes the inode, so future modifications
     * to the inode will not affect the write batch.
     *
     * @param inode the inode to add
     */
    void writeInode(MutableInode<?> inode);

    /**
     * Adds an inode removal operation to the write batch.
     *
     * @param key the key to remove
     */
    void removeInode(Long key);

    /**
     * Adds an edge to the write batch.
     *
     * @param parentId the parent id
     * @param childName the child name
     * @param childId the child id
     */
    void addChild(Long parentId, String childName, Long childId);

    /**
     * Adds an edge removal operation to the write batch.
     *
     * @param parentId the parent id
     * @param childName the child id
     */
    void removeChild(Long parentId, String childName);

    /**
     * Performs the batched write.
     */
    void commit();
  }

  /**
   * Arguments for creating an inode store.
   */
  class InodeStoreArgs {
    private final InodeLockManager mLockManager;
    private final AlluxioConfiguration mConf;

    /**
     * @param lockManager inode lock manager
     * @param conf configuration
     */
    public InodeStoreArgs(InodeLockManager lockManager, AlluxioConfiguration conf) {
      mLockManager = lockManager;
      mConf = conf;
    }

    /**
     * @return the inode lock manager
     */
    public InodeLockManager getLockManager() {
      return mLockManager;
    }

    /**
     * @return the configuration
     */
    public AlluxioConfiguration getConf() {
      return mConf;
    }
  }

  /**
   * Factory for creating inode stores.
   */
  interface Factory extends Function<InodeStoreArgs, InodeStore> {}
}
