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

import static java.util.stream.Collectors.toSet;

import alluxio.collections.TwoKeyConcurrentMap;
import alluxio.concurrent.LockMode;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.file.meta.Edge;
import alluxio.master.file.meta.EdgeEntry;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.file.meta.MutableInodeFile;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.ReadOption;
import alluxio.master.metastore.heap.HeapInodeStore;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.LockResource;
import alluxio.resource.RWLockResource;
import alluxio.util.ConfigurationUtils;
import alluxio.util.ObjectSizeCalculator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * An inode store which caches inode tree metadata and delegates to another inode store for cache
 * misses.
 * <p>
 * We call the other inode store the backing store. Backing store operations are much slower than
 * on-heap operations, so we aim to serve as many requests as possible from the cache.
 * <p>
 * When the inode tree fits completely within the cache, we never interact with the backing store,
 * so performance should be similar to {@link HeapInodeStore}. Once the cache reaches the high
 * watermark, we begin evicting metadata to the backing store, and performance becomes dependent on
 * cache hit rate and backing store performance.
 *
 * <h1>Implementation</h1>
 * <p>
 * The CachingInodeStore uses 3 caches: an inode cache, and edge cache, and a listing cache. The
 * inode cache stores inode metadata such as inode names, permissions, and sizes. The edge cache
 * stores edge metadata, i.e. which inodes are children of which other inodes. The listing cache
 * caches the results of calling getChildren.
 * <p>
 * See the javadoc for {@link InodeCache}, {@link EdgeCache}, and {@link ListingCache} for details
 * about their inner workings.
 */
@ThreadSafe
public final class CachingInodeStore implements InodeStore, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CachingInodeStore.class);

  private final InodeStore mBackingStore;
  private final InodeLockManager mLockManager;

  // Cache recently-accessed inodes.
  @VisibleForTesting
  final InodeCache mInodeCache;

  // Cache recently-accessed inode tree edges.
  @VisibleForTesting
  final EdgeCache mEdgeCache;

  @VisibleForTesting
  final ListingCache mListingCache;

  // Starts true, but becomes permanently false if we ever need to spill metadata to the backing
  // store. When true, we can optimize lookups for non-existent inodes because we don't need to
  // check the backing store. We can also optimize getChildren by skipping the range query on the
  // backing store.
  private volatile boolean mBackingStoreEmpty;

  /**
   * @param backingStore the backing inode store
   * @param lockManager  inode lock manager
   */
  public CachingInodeStore(InodeStore backingStore, InodeLockManager lockManager) {
    mBackingStore = backingStore;
    mLockManager = lockManager;
    AlluxioConfiguration conf = ServerConfiguration.global();
    int maxSize = conf.getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE);
    Preconditions.checkState(maxSize > 0,
        "Maximum cache size %s must be positive, but is set to %s",
        PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE.getName(), maxSize);
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

    mBackingStoreEmpty = true;
    CacheConfiguration cacheConf = CacheConfiguration.newBuilder().setMaxSize(maxSize)
        .setHighWaterMark(highWaterMark).setLowWaterMark(lowWaterMark)
        .setEvictBatchSize(conf.getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_EVICT_BATCH_SIZE))
        .build();
    mInodeCache = new InodeCache(cacheConf);
    mEdgeCache = new EdgeCache(cacheConf);
    mListingCache = new ListingCache(cacheConf);
    if (conf.getBoolean(PropertyKey.MASTER_METRICS_HEAP_ENABLED)) {
      MetricsSystem.registerCachedGaugeIfAbsent(MetricKey.MASTER_INODE_HEAP_SIZE.getName(),
          () -> {
            try {
              return ObjectSizeCalculator.getObjectSize(mInodeCache.mMap,
                  ImmutableSet.of(Long.class, MutableInodeFile.class, MutableInodeDirectory.class))
                  + ObjectSizeCalculator.getObjectSize(mEdgeCache.mMap,
                  ImmutableSet.of(Long.class, Edge.class))
                  + ObjectSizeCalculator.getObjectSize(mListingCache.mMap,
                  ImmutableSet.of(Long.class, ListingCache.ListingCacheEntry.class));
            } catch (NullPointerException e) {
              LOG.info(Throwables.getStackTraceAsString(e));
              throw e;
            }
          });
    }
  }

  @Override
  public Optional<MutableInode<?>> getMutable(long id, ReadOption option) {
    return mInodeCache.get(id, option);
  }

  @Override
  public void remove(Long inodeId) {
    mInodeCache.remove(inodeId);
  }

  @Override
  public void writeInode(MutableInode<?> inode) {
    mInodeCache.put(inode.getId(), inode);
  }

  @Override
  public void writeNewInode(MutableInode<?> inode) {
    if (inode.isDirectory()) {
      mListingCache.addEmptyDirectory(inode.getId());
    }
    mInodeCache.put(inode.getId(), inode);
  }

  @Override
  public void clear() {
    mInodeCache.clear();
    mEdgeCache.clear();
    mBackingStore.clear();
  }

  @Override
  public void addChild(long parentId, String childName, Long childId) {
    mEdgeCache.put(new Edge(parentId, childName), childId);
  }

  @Override
  public void removeChild(long parentId, String name) {
    mEdgeCache.remove(new Edge(parentId, name));
  }

  @Override
  public Iterable<Long> getChildIds(Long inodeId, ReadOption option) {
    return () -> mListingCache.getChildIds(inodeId, option).iterator();
  }

  @Override
  public Optional<Long> getChildId(Long inodeId, String name, ReadOption option) {
    return mEdgeCache.get(new Edge(inodeId, name), option);
  }

  @Override
  public Optional<Inode> getChild(Long inodeId, String name, ReadOption option) {
    return mEdgeCache.get(new Edge(inodeId, name), option).flatMap(this::get);
  }

  @Override
  public boolean hasChildren(InodeDirectoryView inode, ReadOption option) {
    Optional<Collection<Long>> cached = mListingCache.getCachedChildIds(inode.getId());
    if (cached.isPresent()) {
      return !cached.get().isEmpty();
    }
    return !mListingCache.getDataFromBackingStore(inode.getId(), option).isEmpty()
        || mBackingStore.hasChildren(inode);
  }

  @VisibleForTesting
  @Override
  public Set<EdgeEntry> allEdges() {
    return mEdgeCache.allEdges();
  }

  @VisibleForTesting
  @Override
  public Set<MutableInode<?>> allInodes() {
    return mInodeCache.allInodes();
  }

  @Override
  public void close() {
    Closer closer = Closer.create();
    // Close the backing store last so that cache eviction threads don't hit errors.
    closer.register(mBackingStore);
    closer.register(mInodeCache);
    closer.register(mEdgeCache);
    try {
      closer.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.CACHING_INODE_STORE;
  }

  @Override
  public void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException {
    LOG.info("Flushing inodes to backing store");
    mInodeCache.flush();
    mEdgeCache.flush();
    LOG.info("Finished flushing inodes to backing store");
    mBackingStore.writeToCheckpoint(output);
  }

  @Override
  public void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    mInodeCache.clear();
    mEdgeCache.clear();
    mListingCache.clear();
    mBackingStore.restoreFromCheckpoint(input);
    mBackingStoreEmpty = false;
  }

  /**
   * Cache for inode metadata.
   * <p>
   * The cache supports high concurrency across different inode ids, but requires external
   * synchronization for operations on the same inode id. All inodes modifications must hold at
   * least an mLockManager read lock on the modified inode. This allows the cache to flush inodes
   * asynchronously by acquiring a write lock before serializing the inode.
   */
  @VisibleForTesting
  class InodeCache extends Cache<Long, MutableInode<?>> {
    public InodeCache(CacheConfiguration conf) {
      super(conf, "inode-cache", MetricKey.MASTER_INODE_CACHE_EVICTIONS,
          MetricKey.MASTER_INODE_CACHE_HITS, MetricKey.MASTER_INODE_CACHE_LOAD_TIMES,
          MetricKey.MASTER_INODE_CACHE_MISSES, MetricKey.MASTER_INODE_CACHE_SIZE);
    }

    @Override
    protected Optional<MutableInode<?>> load(Long id) {
      if (mBackingStoreEmpty) {
        return Optional.empty();
      }
      return mBackingStore.getMutable(id, ReadOption.defaults());
    }

    @Override
    protected void writeToBackingStore(Long key, MutableInode<?> value) {
      mBackingStoreEmpty = false;
      mBackingStore.writeInode(value);
    }

    @Override
    protected void removeFromBackingStore(Long key) {
      if (!mBackingStoreEmpty) {
        mBackingStore.remove(key);
      }
    }

    @Override
    protected void flushEntries(List<Entry> entries) {
      mBackingStoreEmpty = false;
      boolean useBatch = entries.size() > 0 && mBackingStore.supportsBatchWrite();
      try (WriteBatch batch = useBatch ? mBackingStore.createWriteBatch() : null) {
        for (Entry entry : entries) {
          Long inodeId = entry.mKey;
          Optional<RWLockResource> lockOpt = mLockManager.tryLockInode(inodeId, LockMode.WRITE);
          if (!lockOpt.isPresent()) {
            continue;
          }
          try (LockResource lr = lockOpt.get()) {
            if (entry.mValue == null) {
              if (useBatch) {
                batch.removeInode(inodeId);
              } else {
                mBackingStore.remove(inodeId);
              }
            } else {
              if (useBatch) {
                batch.writeInode(entry.mValue);
              } else {
                mBackingStore.writeInode(entry.mValue);
              }
            }
            entry.mDirty = false;
          }
        }
        if (useBatch) {
          batch.commit();
        }
      }
    }

    private Set<MutableInode<?>> allInodes() {
      Set<MutableInode<?>> cached = mInodeCache.getCacheMap().values().stream()
          .filter(entry -> entry.mValue != null).map(entry -> entry.mValue).collect(toSet());
      Set<Long> unflushedRemoves = mInodeCache.getCacheMap().values().stream()
          .filter(entry -> entry.mValue == null).map(entry -> entry.mKey).collect(toSet());
      Set<MutableInode<?>> flushed = mBackingStore.allInodes().stream()
          .filter(inode -> !unflushedRemoves.contains(inode.getId())).collect(toSet());
      return Sets.union(cached, flushed);
    }
  }

  /**
   * Cache for edge metadata.
   * <p>
   * The edge cache is responsible for managing the mapping from (parentId, childName) to childId.
   * This works similarly to the inode cache.
   * <p>
   * To support getChildIds, the edge cache maintains two indexes: - mIdToChildMap indexes the cache
   * by parent id for fast lookups. - mUnflushedDeletes indexes the cache's "removal" entries by
   * parent id. Removal entries exist for edges which have been removed from the cache, but not yet
   * removed from the backing store.
   */
  @VisibleForTesting
  class EdgeCache extends Cache<Edge, Long> {
    // Indexes non-removed cache entries by parent id. The inner map is from child name to child id
    @VisibleForTesting
    TwoKeyConcurrentMap<Long, String, Long, Map<String, Long>>
        mIdToChildMap = new TwoKeyConcurrentMap<>(() -> new ConcurrentHashMap<>(4));
    // Indexes removed cache entries by parent id. The inner set contains the names of deleted
    // children.
    @VisibleForTesting
    Map<Long, Set<String>> mUnflushedDeletes = new ConcurrentHashMap<>();

    public EdgeCache(CacheConfiguration conf) {
      super(conf, "edge-cache", MetricKey.MASTER_EDGE_CACHE_EVICTIONS,
          MetricKey.MASTER_EDGE_CACHE_HITS, MetricKey.MASTER_EDGE_CACHE_LOAD_TIMES,
          MetricKey.MASTER_EDGE_CACHE_MISSES, MetricKey.MASTER_EDGE_CACHE_SIZE);
    }

    /**
     * Gets the child ids for an inode. This searches the on-heap cache as well as the backing
     * store.
     * <p>
     * Consistency guarantees
     * <p>
     * 1. getChildIds will return all children that existed before getChildIds was invoked. If a
     * child is concurrently removed during the call to getChildIds, it is undefined whether it gets
     * found. 2. getChildIds will never return a child that was removed before getChildIds was
     * invoked. If a child is concurrently added during the call to getChildIds, it is undefined
     * whether it gets found.
     *
     * @param inodeId the inode to get the children for
     * @param option  the read options
     * @return the children
     */
    public Map<String, Long> getChildIds(Long inodeId, ReadOption option) {
      if (mBackingStoreEmpty) {
        return mIdToChildMap.getOrDefault(inodeId, Collections.emptyMap());
      }
      // This implementation must be careful because edges can be asynchronously evicted from the
      // cache to the backing store. To account for this, we read from the cache before consulting
      // the backing store.
      Map<String, Long> childIds = new HashMap<>();
      mIdToChildMap.getOrDefault(inodeId, Collections.emptyMap()).forEach((name, id) -> {
        childIds.put(name, id);
      });
      // Copy the list of unflushed deletes before reading the backing store to prevent racing async
      // deletion.
      Set<String> unflushedDeletes =
          new HashSet<>(mUnflushedDeletes.getOrDefault(inodeId, Collections.EMPTY_SET));
      // Cannot use mBackingStore.getChildren because it only returns inodes cached in the backing
      // store, causing us to lose inodes stored only in the cache.
      mBackingStore.getChildIds(inodeId).forEach(childId -> {
        CachingInodeStore.this.get(childId, option).map(inode -> {
          if (!unflushedDeletes.contains(inode.getName())) {
            childIds.put(inode.getName(), inode.getId());
          }
          return null;
        });
      });
      return childIds;
    }

    @Override
    protected Optional<Long> load(Edge edge) {
      if (mBackingStoreEmpty) {
        return Optional.empty();
      }
      return mBackingStore.getChildId(edge.getId(), edge.getName());
    }

    @Override
    protected void writeToBackingStore(Edge key, Long value) {
      mBackingStoreEmpty = false;
      mBackingStore.addChild(key.getId(), key.getName(), value);
    }

    @Override
    protected void removeFromBackingStore(Edge key) {
      if (!mBackingStoreEmpty) {
        mBackingStore.removeChild(key.getId(), key.getName());
      }
    }

    @Override
    protected void flushEntries(List<Entry> entries) {
      mBackingStoreEmpty = false;
      boolean useBatch = entries.size() > 0 && mBackingStore.supportsBatchWrite();
      try (WriteBatch batch = useBatch ? mBackingStore.createWriteBatch() : null) {
        for (Entry entry : entries) {
          Edge edge = entry.mKey;
          Optional<RWLockResource> lockOpt = mLockManager.tryLockEdge(edge, LockMode.WRITE);
          if (!lockOpt.isPresent()) {
            continue;
          }
          try (LockResource lr = lockOpt.get()) {
            Long value = entry.mValue;
            if (value == null) {
              if (useBatch) {
                batch.removeChild(edge.getId(), edge.getName());
              } else {
                mBackingStore.removeChild(edge.getId(), edge.getName());
              }
            } else {
              if (useBatch) {
                batch.addChild(edge.getId(), edge.getName(), value);
              } else {
                mBackingStore.addChild(edge.getId(), edge.getName(), value);
              }
            }
            entry.mDirty = false;
          }
        }
        if (useBatch) {
          batch.commit();
        }
      }
    }

    @Override
    protected void onCacheUpdate(Edge edge, Long childId) {
      if (childId == null) {
        mIdToChildMap.removeInnerValue(edge.getId(), edge.getName());
        addToUnflushedDeletes(edge.getId(), edge.getName());
      } else {
        mIdToChildMap.addInnerValue(edge.getId(), edge.getName(), childId);
        removeFromUnflushedDeletes(edge.getId(), edge.getName());
      }
    }

    @Override
    protected void onCacheRemove(Edge edge) {
      mIdToChildMap.removeInnerValue(edge.getId(), edge.getName());
      removeFromUnflushedDeletes(edge.getId(), edge.getName());
    }

    @Override
    protected void onPut(Edge edge, Long childId) {
      mListingCache.addEdge(edge, childId);
    }

    @Override
    protected void onRemove(Edge edge) {
      mListingCache.removeEdge(edge);
    }

    private void addToUnflushedDeletes(long parentId, String childName) {
      mUnflushedDeletes.compute(parentId, (key, children) -> {
        if (children == null) {
          children = ConcurrentHashMap.newKeySet(4);
        }
        children.add(childName);
        return children;
      });
    }

    private void removeFromUnflushedDeletes(Long parentId, String childName) {
      mUnflushedDeletes.computeIfPresent(parentId, (key, children) -> {
        children.remove(childName);
        if (children.isEmpty()) {
          return null;
        }
        return children;
      });
    }

    private Set<EdgeEntry> allEdges() {
      Set<EdgeEntry> cacheEntries =
          mIdToChildMap.flattenEntries((a, b, c) -> new EdgeEntry(a, b, c));
      Set<EdgeEntry> backingStoreEntries =
          mBackingStore.allEdges().stream()
              .filter(edge -> !mUnflushedDeletes
                  .getOrDefault(edge.getParentId(), Collections.emptySet())
                  .contains(edge.getChildName()))
              .collect(toSet());
      return Sets.union(cacheEntries, backingStoreEntries);
    }

    @VisibleForTesting
    void verifyIndices() {
      mMap.forEachValue(1, entry -> {
        if (entry.mValue == null) {
          if (!mUnflushedDeletes.get(entry.mKey.getId()).contains(entry.mKey.getName())) {
            throw new IllegalStateException(
                "Missing entry " + entry.mKey + " in unflushed deletes index");
          }
        } else {
          if (!mIdToChildMap.get(entry.mKey.getId()).get(entry.mKey.getName())
              .equals(entry.mValue)) {
            throw new IllegalStateException(String
                .format("Missing entry %s=%s from id to child map", entry.mKey, entry.mValue));
          }
        }
      });
      mIdToChildMap.flattenEntries((parentId, childName, childId) -> {
        if (!mMap.get(new Edge(parentId, childName)).mValue.equals(childId)) {
          throw new IllegalStateException(String.format(
              "Entry %s->%s=%s exists in the index but not the map", parentId, childName, childId));
        }
        return null;
      });
      mUnflushedDeletes.forEach((id, names) -> {
        for (String name : names) {
          if (mMap.get(new Edge(id, name)).mValue != null) {
            throw new IllegalStateException(String.format(
                "Entry %s->%s exists in the unflushed index but not in the map", id, name));
          }
        }
      });
    }
  }

  /**
   * Cache for caching the results of listing directory children.
   * <p>
   * Unlike the inode and edge caches, this cache is not a source of truth. It just does its best to
   * pre-compute getChildIds results so that they can be served quickly.
   * <p>
   * The listing cache contains a mapping from parent id to a complete set of its children. Listings
   * are always complete (containing all children in both the edge cache and the backing store). To
   * maintain our listings, the edge cache calls {@link ListingCache#addEdge(Edge, Long)} and
   * {@link ListingCache#removeEdge(Edge)} whenever edges are added or removed. When new directories
   * are created, the inode cache calls {@link ListingCache#addEmptyDirectory(long)}. This means
   * that with enough cache space, all list requests can be served without going to the backing
   * store.
   * <p>
   * getChildIds may be called concurrently with operations that add or remove child ids from the
   * directory in question. To account for this, we use a mModified field in ListingCacheEntry to
   * record whether any modifications are made to the directory while we are listing it. If any
   * modifications are detected, we cannot safely cache, so we skip caching.
   * <p>
   * The listing cache tracks its size by weight. The weight for each entry is one plus the size of
   * the listing. Once the weight reaches the high water mark, the first thread to acquire the
   * eviction lock will evict down to the low watermark before computing and caching its result.
   */
  @VisibleForTesting
  class ListingCache {
    private final int mMaxSize;
    private final int mHighWaterMark;
    private final int mLowWaterMark;
    private AtomicLong mWeight = new AtomicLong(0);
    private Lock mEvictionLock = new ReentrantLock();

    StatsCounter mStatsCounter;

    private Map<Long, ListingCacheEntry> mMap = new ConcurrentHashMap<>();
    private Iterator<Map.Entry<Long, ListingCacheEntry>> mEvictionHead = mMap.entrySet().iterator();

    private ListingCache(CacheConfiguration conf) {
      mMaxSize = conf.getMaxSize();
      mHighWaterMark = conf.getHighWaterMark();
      mLowWaterMark = conf.getLowWaterMark();

      mStatsCounter = new StatsCounter(
          MetricKey.MASTER_LISTING_CACHE_EVICTIONS,
          MetricKey.MASTER_LISTING_CACHE_HITS,
          MetricKey.MASTER_LISTING_CACHE_LOAD_TIMES,
          MetricKey.MASTER_LISTING_CACHE_MISSES);
      MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_LISTING_CACHE_SIZE.getName(),
          () -> mWeight.get());
    }

    /**
     * Notifies the cache of a newly created empty directory.
     * <p>
     * This way, we can have a cache hit on the first time the directory is listed.
     *
     * @param inodeId the inode id of the directory
     */
    public void addEmptyDirectory(long inodeId) {
      evictIfNecessary();
      mMap.computeIfAbsent(inodeId, x -> {
        mWeight.incrementAndGet();
        ListingCacheEntry entry = new ListingCacheEntry();
        entry.mChildren = new ConcurrentHashMap<>(4);
        return entry;
      });
    }

    /**
     * Updates the cache for an added edge.
     *
     * @param edge    the edge to add
     * @param childId the child of the edge
     */
    public void addEdge(Edge edge, Long childId) {
      evictIfNecessary();
      mMap.computeIfPresent(edge.getId(), (key, entry) -> {
        entry.mModified = true;
        entry.addChild(edge.getName(), childId);
        return entry;
      });
    }

    /**
     * Updates the cache for a removed edge.
     *
     * @param edge the removed edge
     */
    public void removeEdge(Edge edge) {
      mMap.computeIfPresent(edge.getId(), (key, entry) -> {
        entry.mModified = true;
        entry.removeChild(edge.getName());
        return entry;
      });
    }

    /**
     * Returns the children of the given inode, if the child list is cached. This method only
     * consults the cache, and never looks in the backing store.
     *
     * @param inodeId the directory to list
     * @return the children of the inode, or empty if the child list isn't cached
     */
    public Optional<Collection<Long>> getCachedChildIds(Long inodeId) {
      ListingCacheEntry entry = mMap.get(inodeId);
      if (entry != null && entry.mChildren != null) {
        mStatsCounter.recordHit();
        entry.mReferenced = true;
        return Optional.of(entry.mChildren.values());
      }
      mStatsCounter.recordMiss();
      return Optional.empty();
    }

    /**
     * Gets all children of an inode, falling back on the edge cache if the listing isn't cached.
     *
     * @param inodeId the inode directory id
     * @param option  the read options
     * @return the ids of all children of the directory
     */
    public Collection<Long> getChildIds(Long inodeId, ReadOption option) {
      evictIfNecessary();
      AtomicBoolean createdNewEntry = new AtomicBoolean(false);
      ListingCacheEntry entry = mMap.compute(inodeId, (key, value) -> {
        if (value == null) {
          mStatsCounter.recordMiss();
          if (mWeight.get() >= mMaxSize) {
            return null;
          }
          createdNewEntry.set(true);
          return new ListingCacheEntry();
        }
        mStatsCounter.recordHit();
        value.mReferenced = true;
        return value;
      });
      if (entry != null && entry.mChildren != null) {
        return entry.mChildren.values();
      }
      if (entry == null || !createdNewEntry.get() || option.shouldSkipCache()) {
        // Skip caching if the cache is full or someone else is already caching.
        return getDataFromBackingStore(inodeId, option).values();
      }
      return loadChildren(inodeId, entry, option).values();
    }

    public void clear() {
      mMap.clear();
      mWeight.set(0);
      mEvictionHead = mMap.entrySet().iterator();
    }

    private Map<String, Long> loadChildren(Long inodeId, ListingCacheEntry entry,
        ReadOption option) {
      evictIfNecessary();
      entry.mModified = false;
      Map<String, Long> listing = getDataFromBackingStore(inodeId, option);
      mMap.computeIfPresent(inodeId, (key, value) -> {
        // Perform the update inside computeIfPresent to prevent concurrent modification to the
        // cache entry.
        if (!entry.mModified) {
          entry.mChildren = new ConcurrentHashMap<>(listing);
          mWeight.addAndGet(weight(entry));
          return entry;
        }
        return null;
      });
      return listing;
    }

    private void evictIfNecessary() {
      if (mWeight.get() <= mHighWaterMark) {
        return;
      }
      if (mEvictionLock.tryLock()) {
        try {
          evict();
        } finally {
          mEvictionLock.unlock();
        }
      }
    }

    private void evict() {
      long startTime = System.currentTimeMillis();
      long evictTarget = mWeight.get() - mLowWaterMark;
      AtomicInteger evicted = new AtomicInteger(0);
      while (evicted.get() < evictTarget) {
        if (!mEvictionHead.hasNext()) {
          mEvictionHead = mMap.entrySet().iterator();
        }
        if (!mEvictionHead.hasNext()) {
          break; // cache is empty.
        }
        Entry<Long, ListingCacheEntry> candidate = mEvictionHead.next();
        if (candidate.getValue().mReferenced) {
          candidate.getValue().mReferenced = false;
          continue;
        }
        mMap.compute(candidate.getKey(), (key, entry) -> {
          if (entry != null && entry.mChildren != null) {
            mWeight.addAndGet(-weight(entry));
            evicted.addAndGet(weight(entry));
            return null;
          }
          return entry;
        });
      }
      LOG.debug("Evicted weight={} from listing cache down to weight={} in {}ms", evicted.get(),
          mWeight.get(), System.currentTimeMillis() - startTime);
    }

    private int weight(ListingCacheEntry entry) {
      Preconditions.checkNotNull(entry);
      Preconditions.checkNotNull(entry.mChildren);
      // Add 1 to take the key into account.
      return entry.mChildren.size() + 1;
    }

    private Map<String, Long> getDataFromBackingStore(Long inodeId, ReadOption option) {
      final Stopwatch stopwatch = Stopwatch.createStarted();
      final Map<String, Long> result = mEdgeCache.getChildIds(inodeId, option);
      mStatsCounter.recordLoad(stopwatch.elapsed(TimeUnit.NANOSECONDS));
      return result;
    }

    private class ListingCacheEntry {
      private volatile boolean mModified = false;
      private volatile boolean mReferenced = true;
      // null indicates that we are in the process of loading the children.
      @Nullable
      private volatile Map<String, Long> mChildren = null;

      public void addChild(String name, Long id) {
        if (mChildren != null && mChildren.put(name, id) == null) {
          mWeight.incrementAndGet();
        }
      }

      public void removeChild(String name) {
        if (mChildren != null && mChildren.remove(name) != null) {
          mWeight.decrementAndGet();
        }
      }
    }
  }
}
