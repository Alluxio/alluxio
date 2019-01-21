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

import alluxio.PropertyKey;
import alluxio.conf.InstancedConfiguration;
import alluxio.master.file.meta.Edge;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.InodeTree.LockMode;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.java.HeapInodeStore;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.LockResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An inode store which caches inode tree metadata and delegates to another inode store for cache
 * misses.
 *
 * We call the other inode store the backing store. Backing store operations are much slower than
 * on-heap operations, so we aim to serve as many requests as possible from the cache.
 *
 * When the inode tree fits completely within the cache, we never interact with the backing store,
 * so performance should be similar to {@link HeapInodeStore}. Once the cache reaches the high
 * watermark, we begin evicting metadata to the backing store, and performance becomes dependent on
 * cache hit rate and backing store performance.
 *
 * <h1>Implementation</h1>
 *
 * The CachingInodeStore uses 3 caches: an inode cache, and edge cache, and a listing cache. The
 * inode cache stores inode metadata such as inode names, permissions, and sizes. The edge cache
 * stores edge metadata, i.e. which inodes are children of which other inodes. The listing cache
 * caches the results of calling getChildren.
 *
 * See the javadoc for {@link InodeCache}, {@link EdgeCache}, and {@link ListingCache} for details
 * about their inner workings.
 */
public final class CachingInodeStore implements InodeStore {
  private static final Logger LOG = LoggerFactory.getLogger(CachingInodeStore.class);

  private final InodeStore mBackingStore;
  private final InodeLockManager mLockManager;

  // Cache recently-accessed inodes.
  private final InodeCache mInodeCache;

  // Cache recently-accessed inode tree edges.
  private final EdgeCache mEdgeCache;

  private final ListingCache mListingCache;

  // Starts true, but becomes permanently false if we ever need to spill metadata to the backing
  // store. When true, we can optimize lookups for non-existent inodes because we don't need to
  // check the backing store. We can also optimize getChildren by skipping the range query on the
  // backing store.
  private volatile boolean mFullyCached;

  /**
   * @param backingStore the backing inode store
   * @param lockManager the inode lock manager
   * @param conf configuration
   */
  public CachingInodeStore(InodeStore backingStore, InodeLockManager lockManager,
      InstancedConfiguration conf) {
    mBackingStore = backingStore;
    mLockManager = lockManager;
    int maxSize = conf.getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE);
    int highWaterMark = Math.round(
        maxSize * conf.getFloat(PropertyKey.MASTER_METASTORE_INODE_CACHE_HIGH_WATER_MARK_RATIO));
    int lowWaterMark = Math.round(
        maxSize * conf.getFloat(PropertyKey.MASTER_METASTORE_INODE_CACHE_LOW_WATER_MARK_RATIO));

    mFullyCached = true;
    CacheConfiguration cacheConf = CacheConfiguration.newBuilder()
        .setMaxSize(maxSize)
        .setHighWaterMark(highWaterMark)
        .setLowWaterMark(lowWaterMark)
        .setEvictBatchSize(conf.getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_EVICT_BATCH_SIZE))
        .build();
    mInodeCache = new InodeCache(cacheConf);
    mEdgeCache = new EdgeCache(cacheConf);
    mListingCache = new ListingCache(cacheConf);
  }

  @Override
  public Optional<MutableInode<?>> getMutable(long id) {
    return mInodeCache.get(id);
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
  public long estimateSize() {
    return mBackingStore.estimateSize();
  }

  @Override
  public Iterable<Long> getChildIds(Long inodeId) {
    return () -> mListingCache.getChildIds(inodeId).iterator();
  }

  @Override
  public Optional<Long> getChildId(Long inodeId, String name) {
    return mEdgeCache.get(new Edge(inodeId, name));
  }

  @Override
  public Optional<Inode> getChild(Long inodeId, String name) {
    return mEdgeCache.get(new Edge(inodeId, name)).flatMap(this::get);
  }

  @Override
  public boolean hasChildren(InodeDirectoryView inode) {
    Optional<Collection<Long>> cached = mListingCache.getCachedChildIds(inode.getId());
    if (cached.isPresent()) {
      return !cached.get().isEmpty();
    }
    return !mEdgeCache.getChildIds(inode.getId()).isEmpty() || mBackingStore.hasChildren(inode);
  }

  /**
   * Cache for inode metadata.
   *
   * The cache supports high concurrency across different inode ids, but requires external
   * synchronization for operations on the same inode id. All inodes modifications must hold at
   * least an mLockManager read lock on the modified inode. This allows the cache to flush inodes
   * asynchronously by acquiring a write lock before serializing the inode.
   */
  private class InodeCache extends Cache<Long, MutableInode<?>> {
    public InodeCache(CacheConfiguration conf) {
      super(conf, "inode-cache");
    }

    @Override
    protected Optional<MutableInode<?>> load(Long id) {
      if (mFullyCached) {
        return Optional.empty();
      }
      return mBackingStore.getMutable(id);
    }

    @Override
    protected void writeToBackingStore(Long key, MutableInode<?> value) {
      mBackingStore.writeInode(value);
    }

    @Override
    protected void removeFromBackingStore(Long key) {
      mBackingStore.remove(key);
    }

    @Override
    protected void flushEntries(List<Entry> entries) {
      if (mBackingStore.supportsBatchWrite()) {
        batchFlush(entries);
      }
      for (Entry entry : entries) {
        Optional<LockResource> lockOpt = mLockManager.tryLockInode(entry.mKey, LockMode.WRITE);
        if (!lockOpt.isPresent()) {
          continue;
        }
        try (LockResource lr = lockOpt.get()) {
          if (entry.mValue == null) {
            mBackingStore.remove(entry.mKey);
          } else {
            mBackingStore.writeInode(entry.mValue);
          }
          entry.mDirty = false;
        }
      }
    }

    private void batchFlush(List<Entry> entries) {
      WriteBatch batch = mBackingStore.createWriteBatch();
      for (Entry entry : entries) {
        Optional<LockResource> lockOpt = mLockManager.tryLockInode(entry.mKey, LockMode.WRITE);
        if (!lockOpt.isPresent()) {
          continue;
        }
        try (LockResource lr = lockOpt.get()) {
          if (entry.mValue == null) {
            batch.removeInode(entry.mKey);
          } else {
            batch.writeInode(entry.mValue);
          }
          entry.mDirty = false;
        }
      }
      batch.commit();
    }

    @Override
    protected void onEvict(Long id, MutableInode<?> inode) {
      mFullyCached = false;
    }
  }

  /**
   * Cache for edge metadata.
   *
   * The edge cache is responsible for managing the mapping from (parentId, childName) to childId.
   * This works similarly to the inode cache.
   *
   * To support getChildIds, the edge cache maintains two indexes:
   * - mIdToChildMap indexes the cache by parent id for fast lookups.
   * - mUnflushedDeletes indexes the cache's "removal" entries by parent id. Removal entries exist
   *   for edges which have been removed from the cache, but not yet removed from the backing store.
   */
  private class EdgeCache extends Cache<Edge, Long> {
    private final ConcurrentSkipListMap<String, Long> mEmpty = new ConcurrentSkipListMap<>();

    // Indexes non-removed cache entries by parent id
    private Map<Long, ConcurrentSkipListMap<String, Long>> mIdToChildMap =
        new ConcurrentHashMap<>();
    // Indexes removed cache entries by parent id
    private Map<Long, Set<String>> mUnflushedDeletes = new ConcurrentHashMap<>();

    public EdgeCache(CacheConfiguration conf) {
      super(conf, "edge-cache");
    }

    /**
     * Gets the child ids for an inode. This searches the on-heap cache as well as the backing
     * store.
     *
     * Consistency guarantees
     *
     * 1. getChildIds will return all children that existed before getChildIds was invoked. If a
     * child is concurrently removed during the call to getChildIds, it is undefined whether it gets
     * found.
     * 2. getChildIds will never return a child that was removed before getChildIds was invoked. If
     * a child is concurently added during the call to getChildIds, it is undefined whether it gets
     * found.
     *
     * @param inodeId the inode to get the children for
     * @return the children
     */
    public ConcurrentSkipListMap<String, Long> getChildIds(Long inodeId) {
      if (mFullyCached) {
        return mIdToChildMap.getOrDefault(inodeId, mEmpty);
      }
      // This implementation must be careful because edges can be asynchronously evicted from the
      // cache to the backing store.
      //
      // To prevent races, we copy the cached children and unflushed deletes *before* inspecting the
      // backing store. This fulfills the first consistency guarantee because pre-existing children
      // move in the direction of cache -> backing store, so by checking in this order we are
      // guaranteed to find them. The mUnflushedDeletes map prevents inclusion of inodes which have
      // been removed from the cache, but not yet flushed to the backing store. This could happen
      // if we create, evict, load, then remove an edge. Before the remove is evicted, the edge will
      // be returned by the backing store's getChildIds. mUnflushedDeletes filters out such phantom
      // edges.
      Iterator<Map.Entry<String, Long>> cachedChildren =
          new ArrayList<>(mIdToChildMap.getOrDefault(inodeId, mEmpty).entrySet()).iterator();
      Set<String> unflushedDeletes =
          new HashSet<>(mUnflushedDeletes.getOrDefault(inodeId, Collections.EMPTY_SET));
      Iterator<Long> flushedChildren = mBackingStore.getChildIds(inodeId).iterator();
      ConcurrentSkipListMap<String, Long> result = new ConcurrentSkipListMap<>();

      Map.Entry<String, Long> cached = nextCachedInode(cachedChildren);
      Inode flushed = nextFlushedInode(flushedChildren, unflushedDeletes);
      while (cached != null && flushed != null) {
        int comparison = cached.getKey().compareTo(flushed.getName());
        if (comparison == 0) {
          // De-duplicate children with the same name.
          result.put(cached.getKey(), cached.getValue());
          flushed = nextFlushedInode(flushedChildren, unflushedDeletes);
          cached = nextCachedInode(cachedChildren);
        } else if (comparison < 0) {
          result.put(cached.getKey(), cached.getValue());
          cached = nextCachedInode(cachedChildren);
        } else {
          result.put(flushed.getName(), flushed.getId());
          flushed = nextFlushedInode(flushedChildren, unflushedDeletes);
        }
      }
      while (cached != null) {
        result.put(cached.getKey(), cached.getValue());
        cached = nextCachedInode(cachedChildren);
      }
      while (flushed != null) {
        result.put(flushed.getName(), flushed.getId());
        flushed = nextFlushedInode(flushedChildren, unflushedDeletes);
      }
      return result;
    }

    private Inode nextFlushedInode(Iterator<Long> flushedIterator,
        Set<String> unflushedDeletes) {
      while (flushedIterator.hasNext()) {
        Long id = flushedIterator.next();
        Optional<Inode> inode = CachingInodeStore.this.get(id);
        if (inode.isPresent() && !unflushedDeletes.contains(inode.get().getName())) {
          return inode.get();
        }
      }
      return null;
    }

    private <T> T nextCachedInode(Iterator<T> cacheIterator) {
      return cacheIterator.hasNext() ? cacheIterator.next() : null;
    }

    @Override
    protected Optional<Long> load(Edge edge) {
      if (mFullyCached) {
        return Optional.empty();
      }
      return mBackingStore.getChildId(edge.getId(), edge.getName());
    }

    @Override
    protected void writeToBackingStore(Edge key, Long value) {
      mBackingStore.addChild(key.getId(), key.getName(), value);
    }

    @Override
    protected void removeFromBackingStore(Edge key) {
      mBackingStore.removeChild(key.getId(), key.getName());
    }

    @Override
    protected void flushEntries(List<Entry> entries) {
      boolean useBatch = entries.size() > 0 && mBackingStore.supportsBatchWrite();
      WriteBatch batch = useBatch ? mBackingStore.createWriteBatch() : null;
      for (Entry entry : entries) {
        Edge edge = entry.mKey;
        Optional<LockResource> lockOpt = mLockManager.tryLockEdge(edge, LockMode.WRITE);
        if (!lockOpt.isPresent()) {
          continue;
        }
        try (LockResource lr = lockOpt.get()) {
          if (entry.mValue == null) {
            if (useBatch) {
              batch.removeChild(edge.getId(), edge.getName());
            } else {
              mBackingStore.removeChild(edge.getId(), edge.getName());
            }
          } else {
            if (useBatch) {
              batch.addChild(edge.getId(), edge.getName(), entry.mValue);
            } else {
              mBackingStore.addChild(edge.getId(), edge.getName(), entry.mValue);
            }
          }
          entry.mDirty = false;
        }
      }
      if (useBatch) {
        batch.commit();
      }
    }

    @Override
    protected void onCacheUpdate(Edge edge, Long childId) {
      if (childId == null) {
        removeFromIdToChildMap(edge.getId(), edge.getName());
        addToUnflushedDeletes(edge.getId(), edge.getName());
      } else {
        addToIdToChildMap(edge.getId(), edge.getName(), childId);
        removeFromUnflushedDeletes(edge.getId(), edge.getName());
      }
    }

    @Override
    protected void onNew(Edge edge, Long childId) {
      mListingCache.addEdge(edge, childId);
    }

    @Override
    protected void onRemove(Edge edge) {
      mListingCache.removeEdge(edge);
    }

    @Override
    protected void onEvict(Edge edge, Long childId) {
      mFullyCached = false;
      removeFromIdToChildMap(edge.getId(), edge.getName());
      removeFromUnflushedDeletes(edge.getId(), edge.getName());
    }

    private void addToIdToChildMap(Long parentId, String childName, Long childId) {
      mIdToChildMap.compute(parentId, (key, children) -> {
        if (children == null) {
          children = new ConcurrentSkipListMap<>();
        }
        children.put(childName, childId);
        return children;
      });
    }

    private void removeFromIdToChildMap(Long parentId, String childName) {
      mIdToChildMap.compute(parentId, (key, children) -> {
        if (children == null) {
          return null;
        }
        children.remove(childName);
        if (children.isEmpty()) {
          return null;
        }
        return children;
      });
    }

    private void addToUnflushedDeletes(long parentId, String childName) {
      mUnflushedDeletes.computeIfPresent(parentId, (key, children) -> {
        if (children == null) {
          children = new ConcurrentSkipListSet<>();
        }
        children.add(childName);
        return children;
      });
    }

    private void removeFromUnflushedDeletes(Long parentId, String childName) {
      mUnflushedDeletes.computeIfPresent(parentId, (key, children) -> {
        if (children == null) {
          return null;
        }
        children.remove(childName);
        if (children.isEmpty()) {
          return null;
        }
        return children;
      });
    }
  }

  /**
   * Cache for caching the results of listing directory children.
   *
   * Unlike the inode and edge caches, this cache is not a source of truth. It just does its best to
   * pre-compute getChildIds results so that they can be served quickly.
   *
   * The listing cache contains a mapping from parent id to a complete set of its children. Listings
   * are always complete (containing all children in both the edge cache and the backing store). To
   * maintain our listings, the edge cache calls {@link ListingCache#addEdge(Edge, Long)} and
   * {@link ListingCache#removeEdge(Edge)} whenever edges are added or removed. When new directories
   * are created, the inode cache calls {@link ListingCache#addEmptyDirectory(long)}. This means
   * that with enough cache space, all list requests can be served without going to the backing
   * store.
   *
   * getChildIds may be called concurrently with operations that add or remove child ids from the
   * directory in question. To account for this, we use a mModified field in ListingCacheEntry to
   * record whether any modifications are made to the directory while we are listing it. If any
   * modifications are detected, we cannot safely cache, so we skip caching.
   *
   * The listing cache tracks its size by weight. The weight for each entry is one plus the size of
   * the listing. Once the weight reaches the high water mark, the first thread to acquire the
   * eviction lock will evict down to the low watermark before computing and caching its result.
   */
  private class ListingCache {
    private final int mMaxSize;
    private final int mHighWaterMark;
    private final int mLowWaterMark;
    private AtomicLong mWeight = new AtomicLong(0);
    private Lock mEvictionLock = new ReentrantLock();

    private Map<Long, ListingCacheEntry> mMap = new ConcurrentHashMap<>();
    private Iterator<Map.Entry<Long, ListingCacheEntry>> mEvictionHead = mMap.entrySet().iterator();

    private ListingCache(CacheConfiguration conf) {
      mMaxSize = conf.getMaxSize();
      mHighWaterMark = conf.getHighWaterMark();
      mLowWaterMark = conf.getLowWaterMark();
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName("listing-cache-size"),
          () -> mWeight.get());
    }

    /**
     * Notifies the cache of a newly created empty directory.
     *
     * This way, we can have a cache hit on the first time the directory is listed.
     *
     * @param inodeId the inode id of the directory
     */
    public void addEmptyDirectory(long inodeId) {
      mMap.computeIfAbsent(inodeId, x -> {
        ListingCacheEntry entry = new ListingCacheEntry();
        entry.mChildren = new ConcurrentSkipListMap<>();
        return entry;
      });
    }

    /**
     * Updates the cache for an added edge.
     *
     * @param edge the edge to add
     * @param childId the child of the edge
     */
    public void addEdge(Edge edge, Long childId) {
      mMap.compute(edge.getId(), (key, entry) -> {
        if (entry != null) {
          entry.mModified = true;
          if (entry.mChildren != null) {
            entry.addChild(edge.getName(), childId);
          }
        }
        return entry;
      });
    }

    /**
     * Updates the cache for a removed edge.
     *
     * @param edge the removed edge
     */
    public void removeEdge(Edge edge) {
      mMap.compute(edge.getId(), (key, entry) -> {
        if (entry != null) {
          entry.mModified = true;
          if (entry.mChildren != null) {
            entry.removeChild(edge.getName());
          }
        }
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
        entry.mReferenced = true;
        return Optional.of(entry.mChildren.values());
      }
      return Optional.empty();
    }

    /**
     * Gets all children of an inode, falling back on the edge cache if the listing isn't cached.
     *
     * @param inodeId the inode directory id
     * @return the ids of all children of the directory
     */
    public Collection<Long> getChildIds(Long inodeId) {
      ListingCacheEntry entry = mMap.compute(inodeId, (key, value) -> {
        if (value == null) {
          return new ListingCacheEntry();
        }
        value.mReferenced = true;
        return value;
      });
      if (entry.mChildren != null) {
        return entry.mChildren.values();
      }
      if (mWeight.get() < mMaxSize && entry.mLoading.compareAndSet(false, true)) {
        try {
          if (entry.mChildren != null) {
            return entry.mChildren.values();
          }
          return loadChildren(inodeId, entry).values();
        } finally {
          entry.mLoading.set(false);
        }
      }
      return mEdgeCache.getChildIds(inodeId).values();
    }

    private SortedMap<String, Long> loadChildren(Long inodeId, ListingCacheEntry entry) {
      entry.mModified = false;
      if (mWeight.get() > mHighWaterMark) {
        if (mEvictionLock.tryLock()) {
          try {
            evict();
          } finally {
            mEvictionLock.unlock();
          }
        } else {
          if (mWeight.get() >= mMaxSize) {
            // Cache is full and someone else is evicting, so we skip caching altogether.
            return mEdgeCache.getChildIds(inodeId);
          }
        }
      }

      SortedMap<String, Long> listing = mEdgeCache.getChildIds(inodeId);
      mMap.computeIfPresent(inodeId, (key, children) -> {
        // Perform the update inside computeIfPresent to prevent concurrent modification to the
        // cache entry.
        if (!entry.mModified) {
          entry.mChildren = new ConcurrentSkipListMap<>(listing);
          mWeight.addAndGet(entry.mChildren.size() + 1);
        }
        return children;
      });
      return listing;
    }

    private void evict() {
      long startTime = System.currentTimeMillis();
      long toEvict = mMap.size() - mLowWaterMark;
      while (toEvict > 0) {
        if (!mEvictionHead.hasNext()) {
          mEvictionHead = mMap.entrySet().iterator();
        }
        if (!mEvictionHead.hasNext()) {
          break; // cache is empty.
        }
        Entry<Long, ListingCacheEntry> candidate = mEvictionHead.next();
        if (candidate.getValue().mReferenced) {
          candidate.getValue().mReferenced = false;
        }
        mMap.compute(candidate.getKey(), (key, value) -> {
          if (value != null && value.mChildren != null) {
            mWeight.addAndGet(-(value.mChildren.size() + 1));
            return null;
          }
          return value;
        });
      }
      LOG.debug("Evicted weight={} from listing cache down to weight={} in {}ms", toEvict,
          mMap.size(), System.currentTimeMillis() - startTime);
    }

    private class ListingCacheEntry {
      private final AtomicBoolean mLoading = new AtomicBoolean(false);
      private volatile boolean mModified = false;
      private volatile boolean mReferenced = false;
      private volatile ConcurrentSkipListMap<String, Long> mChildren = null;

      public void addChild(String name, Long id) {
        if (mChildren.put(name, id) == null) {
          mWeight.incrementAndGet();
        }
      }

      public void removeChild(String name) {
        if (mChildren.remove(name) != null) {
          mWeight.decrementAndGet();
        }
      }
    }
  }
}
