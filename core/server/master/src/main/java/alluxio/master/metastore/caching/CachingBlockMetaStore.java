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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.metastore.BlockMetaStore;
import alluxio.master.metastore.heap.HeapBlockMetaStore;
import alluxio.master.metastore.rocks.RocksBlockMetaStore;
import alluxio.metrics.MetricKey;
import alluxio.proto.meta.Block.BlockLocation;
import alluxio.proto.meta.Block.BlockMeta;
import alluxio.resource.CloseableIterator;
import alluxio.util.ConfigurationUtils;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A block store which caches block metadata and delegates to another block store for cache
 * misses.
 * <p>
 * We call the other block store the backing store. Backing store operations are much slower than
 * on-heap operations, so we aim to serve as many requests as possible from the cache.
 * <p>
 * When the block fits completely within the cache, we never interact with the backing store,
 * so performance should be similar to {@link HeapBlockMetaStore}. Once the cache reaches the high
 * watermark, we begin evicting metadata to the backing store, and performance becomes dependent on
 * cache hit rate and backing store performance.
 *
 * <h1>Implementation</h1>
 * <p>
 * The CachingBlockMetaStore uses 1 cache: a block meta cache. The
 * block meta cache stores block metadata such as block length.
 * <p>
 * See the javadoc for {@link BlockCache} for details
 * about their inner workings.
 */
@ThreadSafe
public class CachingBlockMetaStore implements BlockMetaStore {
  private final RocksBlockMetaStore mBackingStore;
  private volatile boolean mBackingStoreEmpty;
  private BlockCache mBlockCache;

  /**
   * @param blockMetaStore the backing block meta store
   */
  public CachingBlockMetaStore(RocksBlockMetaStore blockMetaStore) {
    mBackingStore = blockMetaStore;
    AlluxioConfiguration conf = Configuration.global();
    int maxSize = conf.getInt(PropertyKey.MASTER_METASTORE_BLOCK_CACHE_MAX_SIZE);
    Preconditions.checkState(maxSize > 0,
        "Maximum cache size %s must be positive, but is set to %s",
        PropertyKey.MASTER_METASTORE_BLOCK_CACHE_MAX_SIZE.getName(), maxSize);
    float highWaterMarkRatio = ConfigurationUtils.checkRatio(conf,
        PropertyKey.MASTER_METASTORE_INODE_CACHE_HIGH_WATER_MARK_RATIO);
    int highWaterMark = Math.round(maxSize * highWaterMarkRatio);
    float lowWaterMarkRatio = ConfigurationUtils.checkRatio(conf,
        PropertyKey.MASTER_METASTORE_INODE_CACHE_LOW_WATER_MARK_RATIO);
    Preconditions.checkState(lowWaterMarkRatio <= highWaterMarkRatio,
        "low water mark ratio (%s=%s) must not exceed high water mark ratio (%s=%s)",
        PropertyKey.MASTER_METASTORE_INODE_CACHE_LOW_WATER_MARK_RATIO.getName(), lowWaterMarkRatio,
        PropertyKey.MASTER_METASTORE_INODE_CACHE_HIGH_WATER_MARK_RATIO, highWaterMarkRatio);
    int lowWaterMark = Math.round(maxSize * lowWaterMarkRatio);
    CacheConfiguration cacheConf = CacheConfiguration.newBuilder().setMaxSize(maxSize)
        .setHighWaterMark(highWaterMark).setLowWaterMark(lowWaterMark)
        .setEvictBatchSize(conf.getInt(PropertyKey.MASTER_METASTORE_BLOCK_CACHE_EVICT_BATCH_SIZE))
        .build();
    mBlockCache = new BlockCache(cacheConf);
    mBackingStoreEmpty = false;
  }

  @Override
  public Optional<BlockMeta> getBlock(long id) {
    return mBlockCache.get(id);
  }

  @Override
  public void putBlock(long id, BlockMeta meta) {
    mBlockCache.put(id, meta);
  }

  @Override
  public void removeBlock(long id) {
    mBlockCache.remove(id);
  }

  @Override
  public void clear() {
    mBackingStore.clear();
    mBlockCache.clear();
  }

  @Override
  public List<BlockLocation> getLocations(long id) {
    return mBackingStore.getLocations(id);
  }

  @Override
  public void addLocation(long id, BlockLocation location) {
    mBackingStore.addLocation(id, location);
  }

  @Override
  public void removeLocation(long blockId, long workerId) {
    mBackingStore.removeLocation(blockId, workerId);
  }

  @Override
  public void close() {
    Closer closer = Closer.create();
    // Close the backing store last so that cache eviction threads don't hit errors.
    closer.register(mBackingStore);
    closer.register(mBlockCache);
    try {
      closer.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long size() {
    return mBackingStore.size();
  }

  @Override
  public CloseableIterator<Block> getCloseableIterator() {
    return mBackingStore.getCloseableIterator();
  }

  class BlockCache extends Cache<Long, BlockMeta> {
    public BlockCache(CacheConfiguration conf) {
      super(conf, "block-cache", MetricKey.MASTER_BLOCK_META_CACHE_EVICTIONS,
          MetricKey.MASTER_BLOCK_META_CACHE_HITS, MetricKey.MASTER_BLOCK_META_CACHE_LOAD_TIMES,
          MetricKey.MASTER_BLOCK_META_CACHE_MISSES, MetricKey.MASTER_BLOCK_META_CACHE_SIZE);
    }

    @Override
    protected Optional<BlockMeta> load(Long id) {
      if (mBackingStoreEmpty) {
        return Optional.empty();
      }
      return mBackingStore.getBlock(id);
    }

    @Override
    protected void writeToBackingStore(Long key, BlockMeta value) {
      mBackingStoreEmpty = false;
      mBackingStore.putBlock(key, value);
    }

    @Override
    protected void removeFromBackingStore(Long key) {
      if (!mBackingStoreEmpty) {
        mBackingStore.removeBlock(key);
      }
    }

    @Override
    protected void flushEntries(List<Entry> entries) {
      mBackingStoreEmpty = false;
      boolean useBatch = entries.size() > 0 && mBackingStore.supportsBatchWrite();
      try (WriteBatch batch = useBatch ? mBackingStore.createWriteBatch() : null) {
        for (Entry entry : entries) {
          if (entry.mValue == null) {
            if (useBatch) {
              batch.removeBlock(entry.mKey);
            } else {
              mBackingStore.removeBlock(entry.mKey);
            }
          } else {
            if (useBatch) {
              batch.putBlock(entry.mKey, entry.mValue);
            } else {
              mBackingStore.removeBlock(entry.mKey);
            }
          }
          entry.mDirty = false;
        }
        if (useBatch) {
          batch.commit();
        }
      }
    }
  }
}
