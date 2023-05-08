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

package alluxio.master.metastore.caching;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.file.meta.Edge;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.metastore.ReadOption;
import alluxio.master.metastore.rocks.RocksInodeStore;

import com.google.common.base.Preconditions;
import org.cache2k.Cache2kBuilder;

import java.util.Optional;

/**
 * A cache that only caches inodes.
 */
public class BasicInodeCache2k extends RocksInodeStore {

  org.cache2k.Cache<Long, InodeItem> mInodeCache;
  org.cache2k.Cache<Edge, Long> mEdgeCache;

  static class InodeItem {
    MutableInode<?> mItem;

    InodeItem(MutableInode<?> item) {
      mItem = item;
    }
  }

  /**
   * Creates and initializes a rocks block store.
   *
   * @param baseDir the base directory in which to store inode metadata
   */
  public BasicInodeCache2k(String baseDir) {
    super(baseDir);

    int maxSize = Configuration.getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE);
    Preconditions.checkState(maxSize > 0,
        "Maximum cache size %s must be positive, but is set to %s",
        PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE.getName(), maxSize);

    mInodeCache = new Cache2kBuilder<Long, InodeItem>() {}
        .eternal(true).entryCapacity(maxSize).build();
    mEdgeCache = new Cache2kBuilder<Edge, Long>() {}
        .eternal(true).entryCapacity(maxSize).build();
  }

  @Override
  public Optional<MutableInode<?>> getMutable(long inodeId, ReadOption option) {
    return Optional.ofNullable(mInodeCache.asMap().computeIfAbsent(inodeId, id ->
      BasicInodeCache2k.super.getMutable(inodeId, option).map(InodeItem::new)
          .orElse(null))).map(nxt -> nxt.mItem);
  }

  @Override
  public void remove(Long inodeId) {
    mInodeCache.asMap().compute(inodeId, (id, inode) -> {
      BasicInodeCache2k.super.remove(inodeId);
      return null;
    });
  }

  @Override
  public void writeInode(MutableInode<?> inode) {
    mInodeCache.asMap().compute(inode.getId(), (id, oldInode) -> {
      BasicInodeCache2k.super.writeInode(inode);
      return new InodeItem(inode);
    });
  }

  @Override
  public Optional<Long> getChildId(Long inodeId, String name, ReadOption option) {
    Edge edge = new Edge(inodeId, name);
    return Optional.ofNullable(mEdgeCache.asMap().computeIfAbsent(edge, id ->
        BasicInodeCache2k.super.getChildId(inodeId, name, option).orElse(null)));
  }

  @Override
  public void addChild(long parentId, String childName, Long childId) {
    Edge edge = new Edge(parentId, childName);
    mEdgeCache.asMap().compute(edge, (id, oldEdge) -> {
      BasicInodeCache2k.super.addChild(parentId, childName, childId);
      return childId;
    });
  }

  @Override
  public void removeChild(long parentId, String name) {
    Edge edge = new Edge(parentId, name);
    mEdgeCache.asMap().compute(edge, (id, oldEdge) -> {
      BasicInodeCache2k.super.removeChild(parentId, name);
      return null;
    });
  }
}
