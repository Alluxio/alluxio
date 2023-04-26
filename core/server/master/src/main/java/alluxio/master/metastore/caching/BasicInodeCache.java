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
import alluxio.master.file.meta.MutableInode;
import alluxio.master.metastore.ReadOption;
import alluxio.master.metastore.rocks.RocksInodeStore;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;

import java.util.Optional;

/**
 * A cache that only caches inodes.
 */
public class BasicInodeCache extends RocksInodeStore {

  com.github.benmanes.caffeine.cache.Cache<Long, MutableInode<?>> mCache;

  /**
   * Creates and initializes a rocks block store.
   *
   * @param baseDir the base directory in which to store inode metadata
   */
  public BasicInodeCache(String baseDir) {
    super(baseDir);

    int maxSize = Configuration.getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE);
    Preconditions.checkState(maxSize > 0,
        "Maximum cache size %s must be positive, but is set to %s",
        PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE.getName(), maxSize);

    mCache = Caffeine.newBuilder().initialCapacity(maxSize).maximumSize(maxSize).build();
  }

  @Override
  public Optional<MutableInode<?>> getMutable(long inodeId, ReadOption option) {
    return Optional.ofNullable(mCache.asMap().computeIfAbsent(inodeId, id ->
      BasicInodeCache.super.getMutable(inodeId, option).orElse(null)));
  }

  @Override
  public void remove(Long inodeId) {
    mCache.asMap().computeIfPresent(inodeId, (id, inode) -> {
      BasicInodeCache.super.remove(inodeId);
      return null;
    });
  }

  @Override
  public void writeInode(MutableInode<?> inode) {
    mCache.asMap().compute(inode.getId(), (id, oldInode) -> {
      BasicInodeCache.super.writeInode(inode);
      return inode;
    });
  }

  @Override
  public boolean supportsBatchWrite() {
    return false;
  }
}
