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

import alluxio.collections.Pair;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.MutableInode;
import alluxio.proto.meta.InodeMeta;

import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.util.Optional;
import java.util.function.Function;

/**
 * Inode metadata storage.
 *
 * The inode store manages metadata about individual inodes, as well as the parent-child
 * relationships between them.
 *
 * Writes to the inode store happen atomically, so it is safe to concurrently read and write the
 * same key. It is undefined whether the read will see the old value or the new value.
 *
 * The inode store needs a way to temporarily prevent modification to individual inodes or edges. It
 * does this by acquiring inode lock manager write locks. This requires that the user acquires inode
 * manager locks when making any modifications to the inode store.
 */
@ThreadSafe
public interface KVInodeStore extends KVReadOnlyInodeStore, Closeable {
  /**
   * Gets a mutable representation of the specified inode.
   *
   * Mutating the returned inode requires holding an inode lock manager read or write lock on the
   * inode.
   *
   * @param id an inode id
   * @param option read options
   * @return the inode with the given id, if it exists
   */
  Optional<MutableInode<?>> getMutable(long id, String name, ReadOption option);

  default Optional<MutableInode<?>> getMutable(long id, String name) {
    return getMutable(id, name, ReadOption.defaults());
  }

  @Override
  default Optional<Inode> get(long id, String name, ReadOption option) {
    return getMutable(id, name, option).map(Inode::wrap);
  }

  /**
   * Removes an inode from the inode store. This does *not* remove the edge leading to the inode.
   *
   * This method requires an inode lock manager read or write lock on the removed inode.
   *
   * @param inode an inode to remove
   */
  default void remove(InodeView inode) {
    removeChild(inode.getParentId(), inode.getName(), inode.getId());
  }

  /**
   * Adds the given inode, or overwrites it if it already exists.
   *
   * If it is known that the inode is new, prefer {@link #writeNewInode(MutableInode)}.
   *
   * This method requires an inode lock manager read or write lock on the written inode.
   *
   * @param inode the inode to write
   */
  void writeInode(MutableInode<?> inode);

  void writeInodeCacheAttri(InodeMeta.InodeCacheAttri inodeCacheAttri);

  /**
   * Adds a new inode.
   *
   * This method is similar to {@link #writeInode(MutableInode)}, but with an added information that
   * the inode is new. This allows some inode stores to perform extra optimizations.
   *
   * This method requires an inode lock manager read or write lock on the written inode.
   *
   * @param inode the inode to write
   */
  default void writeNewInode(MutableInode<?> inode) {
    writeInode(inode);
  }

  Optional<Pair<Long, String>> getEdgeToParent(long childId);

  void removeEdge(long childId);

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
   * Removes a child from a parent inode.
   *
   * This method requires an inode lock manager read or write locks on the removed edge.
   *
   * @param parentId the parent inode id
   * @param name the child name
   */
  void removeChild(long parentId, String name, long id);

  void removeChild(long parentId, String name);

  void addChild(long parentId, String childName, long childId);

  @Override
  default void close() {}

  /**
   * Used to perform batched writes. Call {@link #createWriteBatch()} to use batched writes.
   *
   * Write batches may or may not be applied atomically.
   */
  interface WriteBatch extends AutoCloseable {
    /**
     * Adds an inode to the write batch. This method serializes the inode, so future modifications
     * to the inode will not affect the write batch.
     *
     * @param inode the inode to add
     */
    void writeInode(MutableInode<?> inode);

    /**
     * Adds an edge removal operation to the write batch.
     *
     * @param parentId the parent id
     * @param childName the child id
     */
    void removeChild(Long parentId, String childName, Long childId);

    void addChild(long parentId, String childName, long childId);

    /**
     * Performs the batched write.
     */
    void commit();

    @Override
    void close();
  }

  /**
   * Factory for creating inode stores.
   */
  interface Factory extends Function<InodeLockManager, KVInodeStore> {}
}
