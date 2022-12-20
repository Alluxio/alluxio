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

package alluxio.master.metastore.kvstorecaching;

import alluxio.collections.Pair;
import alluxio.collections.TwoKeyConcurrentSortedMap;
import alluxio.concurrent.LockMode;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.file.meta.Edge;
import alluxio.master.file.meta.EdgeEntry;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.file.meta.MutableInodeFile;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.KVInodeStore;
import alluxio.master.metastore.ReadOption;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableIterator;
import alluxio.resource.LockResource;
import alluxio.resource.RWLockResource;
import alluxio.util.ConfigurationUtils;
import alluxio.util.ObjectSizeCalculator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.NotSupportedException;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is going to handle the distributed environment.
 */
@ThreadSafe
public final class KVCachingInodeStore implements InodeStore, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KVCachingInodeStore.class);

  private final KVInodeStore mBackingStore;
  private final InodeLockManager mLockManager;

  // Cache recently-accessed inodes.
  @VisibleForTesting
  final InodeCache mInodeCache;

  // Cache recently-accessed inode tree edges.
  @VisibleForTesting
  final EdgeCache mEdgeCache;

  @VisibleForTesting
  final KVListingCache mListingCache;

  private volatile boolean mBackingStoreEmpty;

  /**
   * @param backingStore the backing inode store
   * @param lockManager  inode lock manager
   */
  public KVCachingInodeStore(KVInodeStore backingStore, InodeLockManager lockManager) {
    mBackingStore = backingStore;
    mLockManager = lockManager;
    AlluxioConfiguration conf = Configuration.global();
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

    mBackingStoreEmpty = false;
    alluxio.master.metastore.kvstorecaching.CacheConfiguration cacheConf
        = alluxio.master.metastore.kvstorecaching.CacheConfiguration.newBuilder().setMaxSize(maxSize)
        .setHighWaterMark(highWaterMark).setLowWaterMark(lowWaterMark)
        .setEvictBatchSize(conf.getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_EVICT_BATCH_SIZE))
        .build();
    mInodeCache = new InodeCache(cacheConf);
    mEdgeCache = new EdgeCache(cacheConf);
    mListingCache = new KVListingCache(cacheConf);
    if (conf.getBoolean(PropertyKey.MASTER_METRICS_HEAP_ENABLED)) {
      MetricsSystem.registerCachedGaugeIfAbsent(MetricKey.MASTER_INODE_HEAP_SIZE.getName(),
          () -> {
            try {
              return ObjectSizeCalculator.getObjectSize(mInodeCache.mMap,
                  ImmutableSet.of(Long.class, MutableInodeFile.class, MutableInodeDirectory.class))
                  + ObjectSizeCalculator.getObjectSize(mEdgeCache.mMap,
                  ImmutableSet.of(Long.class, Edge.class))
                  + ObjectSizeCalculator.getObjectSize(mListingCache.mMap,
                  ImmutableSet.of(Long.class, KVListingCache.ListingCacheEntry.class));
            } catch (NullPointerException e) {
              LOG.info(Throwables.getStackTraceAsString(e));
              throw e;
            }
          });
    }
  }

  @Override
  public Optional<MutableInode<?>> getMutable(long id, ReadOption option) {
    Optional<Edge> edge = mEdgeCache.get(id);
    if (!edge.isPresent()) {
      return Optional.empty();
    }
    return getMutable(edge.get().getId(), edge.get().getName());
  }

  public Optional<MutableInode<?>> getMutable(long id, String name) {
    return mInodeCache.get(new Pair<Long, String>(id, name));
  }

  public Optional<Inode> get(long id, String name) {
    return getMutable(id, name).map(Inode::wrap);
  }

  @Override
  public void remove(InodeView inode) {
    mInodeCache.remove(new Pair<Long, String>(inode.getParentId(), inode.getName()));
  }

  @Override
  public void remove(Long inodeId) {
    throw new NotSupportedException("remove(Long inodeId) is not supported");
  }

  @Override
  public void writeInode(MutableInode<?> inode) {
    mEdgeCache.putInCacheOnly(inode.getId(), new Edge(inode.getParentId(), inode.getName()));
    mInodeCache.putInCacheOnly(new Pair<Long, String>(inode.getId(), inode.getName()), inode);
  }

  public void writeInodeToBackend(MutableInode<?> inode) {
    mBackingStore.writeInode(inode);
    mEdgeCache.putInCacheAsClean(inode.getId(), new Edge(inode.getParentId(), inode.getName()));
    mInodeCache.putInCacheAsClean(new Pair<Long, String>(inode.getId(), inode.getName()), inode);
  }

  public void writeNewInodeToBackend(MutableInode<?> inode) {
    mBackingStore.writeInode(inode);
    if (inode.isDirectory()) {
      mListingCache.addEmptyDirectory(inode.getId());
    }
    mInodeCache.putInCacheAsClean(new Pair<Long, String>(inode.getParentId(), inode.getName()), inode);
    mEdgeCache.putInCacheAsClean(inode.getId(), new Edge(inode.getParentId(), inode.getName()));
  }

  @Override
  public void writeNewInode(MutableInode<?> inode) {
    if (inode.isDirectory()) {
      mListingCache.addEmptyDirectory(inode.getId());
    }
    mInodeCache.putInCacheOnly(new Pair<Long, String>(inode.getParentId(), inode.getName()), inode);
    mEdgeCache.putInCacheOnly(inode.getId(), new Edge(inode.getParentId(), inode.getName()));
  }

  @Override
  public void clear() {
    mInodeCache.clear();
    mEdgeCache.clear();
    mBackingStore.clear();
  }

  @Override
  public void addChild(long parentId, String childName, Long childId) {
    mEdgeCache.putInCacheOnly(childId, new Edge(parentId, childName));
  }

  @Override
  public void removeChild(long parentId, String name) {
    Optional<? extends MutableInode> mutableInode = mInodeCache.get(new Pair<Long, String>(parentId, name));
    if (!mutableInode.isPresent()) {
      return;
    }
    mEdgeCache.remove(mutableInode.get().getId());
  }

  public CloseableIterator<String> getChildNames(Long inodeId, ReadOption option) {
    return mListingCache.getChildNames(inodeId, option);
  }

  @Override
  public CloseableIterator<Long> getChildIds(Long inodeId, ReadOption option) {
    throw new NotSupportedException();
    // return mListingCache.getChildIds(inodeId, option);
  }

  @Override
  public Optional<Long> getChildId(Long inodeId, String name, ReadOption option) {
    Optional<MutableInode<?>> optional = mInodeCache.get(new Pair<Long, String>(inodeId, name));
    if (optional.isPresent()) {
      return Optional.of(optional.get().getId());
    }
    return Optional.empty();
  }

  @Override
  public Optional<Inode> getChild(Long inodeId, String name, ReadOption option) {
    // return mEdgeCache.get(new Edge(inodeId, name), option).flatMap(this::get);
    // return mBackingStore.getMutable(inodeId, name).map(Inode::wrap);
    return mInodeCache.get(new Pair<Long, String>(inodeId, name)).map(Inode::wrap);
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
    throw new NotSupportedException();
  }

  @Override
  public Set<MutableInode<?>> allInodes() {
    throw new NotSupportedException("This is not implemented");
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
  }

  @Override
  public void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    // no op
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
  class InodeCache extends alluxio.master.metastore.kvstorecaching.Cache<Pair<Long, String>, MutableInode<?>> {
    public InodeCache(alluxio.master.metastore.kvstorecaching.CacheConfiguration conf) {
      super(conf, "inode-cache", MetricKey.MASTER_INODE_CACHE_EVICTIONS,
          MetricKey.MASTER_INODE_CACHE_HITS, MetricKey.MASTER_INODE_CACHE_LOAD_TIMES,
          MetricKey.MASTER_INODE_CACHE_MISSES, MetricKey.MASTER_INODE_CACHE_SIZE);
    }

    @Override
    protected Optional<MutableInode<?>> load(Pair<Long, String> key) {
      if (mBackingStoreEmpty) {
        return Optional.empty();
      }
      return mBackingStore.getMutable(key.getFirst(), key.getSecond(), ReadOption.defaults());
    }

    @Override
    protected void writeToBackingStore(Pair<Long, String> key, MutableInode<?> value) {
      mBackingStoreEmpty = false;
      // TODO(yyong) so far disable this write
      // mBackingStore.writeInode(value);
    }

    @Override
    protected void removeFromBackingStore(Pair<Long, String> key) {
      // TODO(yyong)  should also remove the cache attribute map
      if (!mBackingStoreEmpty) {
        mBackingStore.removeChild(key.getFirst(), key.getSecond());
      }
    }

    @Override
    protected void flushEntries(List<Entry> entries) {
      mBackingStoreEmpty = false;
      boolean useBatch = entries.size() > 0 && mBackingStore.supportsBatchWrite();
      try (alluxio.master.metastore.KVInodeStore.WriteBatch batch
               = useBatch ? mBackingStore.createWriteBatch() : null) {
        for (Entry entry : entries) {
          Long inodeId = entry.mValue.getId();
          Optional<RWLockResource> lockOpt = mLockManager.tryLockInode(inodeId, LockMode.WRITE);
          if (!lockOpt.isPresent()) {
            continue;
          }
          try (LockResource lr = lockOpt.get()) {
            LOG.info("Evict inode key: {}", entry.mKey);
            if (entry.mValue == null) {
              if (useBatch) {
                batch.removeChild(entry.mKey.getFirst(), entry.mKey.getSecond(), entry.mValue.getId());
              } else {
                mBackingStore.removeChild(entry.mKey.getFirst(), entry.mKey.getSecond());
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
  }

  @Override
  public CloseableIterator<? extends Inode> getChildren(Long inodeId, ReadOption option) {
    CloseableIterator<String> it = getChildNames(inodeId, option);
    Iterator<Inode> iter =  new Iterator<Inode>() {
      private Inode mNext = null;
      @Override
      public boolean hasNext() {
        advance();
        return mNext != null;
      }

      @Override
      public Inode next() {
        if (!hasNext()) {
          throw new NoSuchElementException(
              "No more children in iterator for inode id " + inodeId);
        }
        Inode next = mNext;
        mNext = null;
        return next;
      }

      void advance() {
        while (mNext == null && it.hasNext()) {
          String name = it.next();
          // Make sure the inode metadata still exists
          Optional<Inode> nextInode = get(inodeId, name);
          nextInode.ifPresent(inode -> mNext = inode);
        }
      }
    };
    return CloseableIterator.create(iter, (any) -> it.close());
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
  class EdgeCache extends Cache<Long, Edge> {
    // Indexes non-removed cache entries by parent id. The inner map is from child name to child id
    @VisibleForTesting
    TwoKeyConcurrentSortedMap<Long, String, Long, SortedMap<String, Long>>
        mIdToChildMap = new TwoKeyConcurrentSortedMap<>(ConcurrentSkipListMap::new);
    // Indexes removed cache entries by parent id. The inner set contains the names of deleted
    // children.
    @VisibleForTesting
    Map<Long, Set<String>> mUnflushedDeletes = new ConcurrentHashMap<>();

    public EdgeCache(alluxio.master.metastore.kvstorecaching.CacheConfiguration conf) {
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
    public SortedMap<String, Long> getChildIds(Long inodeId, ReadOption option) {
      if (mBackingStoreEmpty) {
        return mIdToChildMap.getOrDefault(inodeId, Collections.emptySortedMap());
      }
      // This implementation must be careful because edges can be asynchronously evicted from the
      // cache to the backing store. To account for this, we read from the cache before consulting
      // the backing store.
      SortedMap<String, Long> childIds = new ConcurrentSkipListMap<>(mIdToChildMap.getOrDefault(
          inodeId, Collections.emptySortedMap()));
      // Copy the list of unflushed deletes before reading the backing store to prevent racing async
      // deletion.
      Set<String> unflushedDeletes =
          new HashSet<>(mUnflushedDeletes.getOrDefault(inodeId, Collections.emptySet()));
      // Cannot use mBackingStore.getChildren because it only returns inodes cached in the backing
      // store, causing us to lose inodes stored only in the cache.
      try (CloseableIterator<Pair<Long, String>> childIter = mBackingStore.getChildIds(inodeId)) {
        childIter.forEachRemaining(childId -> {
          KVCachingInodeStore.this.get(childId.getFirst(), childId.getSecond()).map(inode -> {
            if (!unflushedDeletes.contains(inode.getName())) {
              childIds.put(inode.getName(), inode.getId());
            }
            return null;
          });
        });
      }
      return childIds;
    }

    @Override
    protected Optional<Edge> load(Long childId) {
      if (mBackingStoreEmpty) {
        return Optional.empty();
      }
      Optional<Pair<Long, String>> optional
          = mBackingStore.getEdgeToParent(childId);
      if (!optional.isPresent()) {
        return Optional.empty();
      }

      return Optional.of(new Edge(optional.get().getFirst(),
          optional.get().getSecond()));
    }

    @Override
    protected void writeToBackingStore(Long key, Edge value) {
      mBackingStoreEmpty = false;
      // mBackingStore.addChild(key.getId(), key.getName(), value);
    }

    @Override
    protected void removeFromBackingStore(Long key) {
      if (!mBackingStoreEmpty) {
        // TODO(yyong) fetch and remove can be optimized
        Optional<Edge> edge = get(key);
        if (!edge.isPresent()) {
          return;
        }
        mBackingStore.removeEdge(key);
      }
    }

    @Override
    protected void flushEntries(List<Entry> entries) {
      mBackingStoreEmpty = false;
      boolean useBatch = entries.size() > 0 && mBackingStore.supportsBatchWrite();
      try (alluxio.master.metastore.KVInodeStore.WriteBatch batch
               = useBatch ? mBackingStore.createWriteBatch() : null) {
        for (Entry entry : entries) {
          Edge edge = entry.mValue;
          Optional<RWLockResource> lockOpt = mLockManager.tryLockEdge(edge, LockMode.WRITE);
          if (!lockOpt.isPresent()) {
            continue;
          }
          try (LockResource lr = lockOpt.get()) {
            Long key = entry.mKey;
            LOG.info("Evict edge key: {}", key);
            if (key == null) {
              if (useBatch) {
                batch.removeChild(edge.getId(), edge.getName(), key);
              } else {
                mBackingStore.removeChild(edge.getId(), edge.getName(), key);
              }
            } else {
              if (useBatch) {
                batch.addChild(edge.getId(), edge.getName(), key);
              } else {
                mBackingStore.addChild(edge.getId(), edge.getName(), key);
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
    protected void onCacheUpdate(Long childId, Edge edge) {
      if (childId == null) {
        mIdToChildMap.removeInnerValue(edge.getId(), edge.getName());
        addToUnflushedDeletes(edge.getId(), edge.getName());
      } else {
        mIdToChildMap.addInnerValue(edge.getId(), edge.getName(), childId);
        removeFromUnflushedDeletes(edge.getId(), edge.getName());
      }
    }

    @Override
    protected void onCacheRemove(Long id) {
      Optional<Edge> edge = get(id);
      if (!edge.isPresent()) {
        return;
      }
      mIdToChildMap.removeInnerValue(edge.get().getId(), edge.get().getName());
      removeFromUnflushedDeletes(edge.get().getId(), edge.get().getName());
    }

    @Override
    protected void onPut(Long childId, Edge edge) {
      mListingCache.addEdge(edge, childId);
    }

    @Override
    protected void onRemove(Long key) {
      Optional<Edge> edge = get(key);
      if (!edge.isPresent()) {
        return;
      }
      mListingCache.removeEdge(edge.get());
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

    @VisibleForTesting
    void verifyIndices() {
      mMap.forEachValue(1, entry -> {
        if (entry.mValue == null) {
          if (!mUnflushedDeletes.get(entry.mValue.getId()).contains(entry.mValue.getName())) {
            throw new IllegalStateException(
                "Missing entry " + entry.mKey + " in unflushed deletes index");
          }
        } else {
          if (!mIdToChildMap.get(entry.mValue.getId()).get(entry.mValue.getName())
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

  public static Iterator<String> sortedMapToIterator(
      SortedMap<String, Long> childrenMap, ReadOption option) {

    if (option.getStartFrom() != null && option.getPrefix() != null) {
      // if the prefix is after readFrom, then we just start the map from
      // the prefix
      if (option.getPrefix().compareTo(option.getStartFrom()) > 0) {
        childrenMap = childrenMap.tailMap(option.getPrefix());
      } else {
        childrenMap = childrenMap.tailMap(option.getStartFrom());
      }
    } else if (option.getStartFrom() != null) {
      childrenMap = childrenMap.tailMap(option.getStartFrom());
    } else if (option.getPrefix() != null) {
      childrenMap = childrenMap.tailMap(option.getPrefix());
    }

    if (option.getPrefix() == null) {
      return childrenMap.keySet().iterator();
    } else {
      // make an iterator that stops once the prefix has been passed
      class PrefixIter implements Iterator<String> {
        final Iterator<Map.Entry<String, Long>> mIter;
        Map.Entry<String, Long> mNxt;

        PrefixIter(Iterator<Map.Entry<String, Long>> iter) {
          mIter = iter;
          checkNext();
        }

        @Override
        public boolean hasNext() {
          return (mNxt != null);
        }

        @Override
        public String next() {
          String val = mNxt.getKey();
          checkNext();
          return val;
        }

        private void checkNext() {
          mNxt = null;
          if (mIter.hasNext()) {
            mNxt = mIter.next();
            if (!mNxt.getKey().startsWith(option.getPrefix())) {
              mNxt = null;
            }
          }
        }
      }

      return new PrefixIter(childrenMap.entrySet().iterator());
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
   * maintain our listings, the edge cache calls {@link KVListingCache#addEdge(Edge, Long)} and
   * {@link KVListingCache#removeEdge(Edge)} whenever edges are added or removed. When new directories
   * are created, the inode cache calls {@link KVListingCache#addEmptyDirectory(long)}. This means
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
  class KVListingCache {
    private final int mMaxSize;
    private final int mHighWaterMark;
    private final int mLowWaterMark;
    private final AtomicLong mWeight = new AtomicLong(0);
    private final Lock mEvictionLock = new ReentrantLock();

    StatsCounter mStatsCounter;

    private final Map<Long, ListingCacheEntry> mMap = new ConcurrentHashMap<>();
    private Iterator<Entry<Long, ListingCacheEntry>> mEvictionHead = mMap.entrySet().iterator();

    private KVListingCache(CacheConfiguration conf) {
      mMaxSize = conf.getMaxSize();
      mHighWaterMark = conf.getHighWaterMark();
      mLowWaterMark = conf.getLowWaterMark();

      mStatsCounter = new StatsCounter(
          MetricKey.MASTER_LISTING_CACHE_EVICTIONS,
          MetricKey.MASTER_LISTING_CACHE_HITS,
          MetricKey.MASTER_LISTING_CACHE_LOAD_TIMES,
          MetricKey.MASTER_LISTING_CACHE_MISSES);
      MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_LISTING_CACHE_SIZE.getName(),
          mWeight::get);
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
        entry.mChildren = new ConcurrentSkipListMap<>();
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

    public CloseableIterator<String> getChildNames(Long inodeId, ReadOption option) {
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
      SortedMap<String, Long> childMap;
      if (entry != null && entry.mChildren != null) {
        childMap = entry.mChildren;
      } else if (entry == null || !createdNewEntry.get() || option.shouldSkipCache()) {
        // Skip caching if the cache is full or someone else is already caching.
        childMap = getDataFromBackingStore(inodeId, option);
      } else {
        childMap = loadChildren(inodeId, entry, option);
      }
      return CloseableIterator.noopCloseable(sortedMapToIterator(childMap, option));
    }

    public void clear() {
      mMap.clear();
      mWeight.set(0);
      mEvictionHead = mMap.entrySet().iterator();
    }

    private SortedMap<String, Long> loadChildren(Long inodeId, ListingCacheEntry entry,
        ReadOption option) {
      evictIfNecessary();
      entry.mModified = false;
      SortedMap<String, Long> listing = getDataFromBackingStore(inodeId, option);
      mMap.computeIfPresent(inodeId, (key, value) -> {
        // Perform the update inside computeIfPresent to prevent concurrent modification to the
        // cache entry.
        if (!entry.mModified) {
          entry.mChildren = new ConcurrentSkipListMap<>(listing);
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

    private SortedMap<String, Long> getDataFromBackingStore(Long inodeId, ReadOption option) {
      final Stopwatch stopwatch = Stopwatch.createStarted();
      final SortedMap<String, Long> result = mEdgeCache.getChildIds(inodeId, option);
      mStatsCounter.recordLoad(stopwatch.elapsed(TimeUnit.NANOSECONDS));
      return result;
    }

    private class ListingCacheEntry {
      private volatile boolean mModified = false;
      private volatile boolean mReferenced = true;
      // null indicates that we are in the process of loading the children.
      @Nullable
      private volatile SortedMap<String, Long> mChildren = null;

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
