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

package alluxio.master.metastore.rocks;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.file.meta.EdgeEntry;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.ReadOption;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.meta.InodeMeta;
import alluxio.resource.CloseableIterator;
import alluxio.util.io.PathUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.HashLinkedListMemTableConfig;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
<<<<<<< HEAD
||||||| parent of 9f152c554b (Fix Rocksdb thread safety using refcount(no lock))
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
=======
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
>>>>>>> 9f152c554b (Fix Rocksdb thread safety using refcount(no lock))
import javax.annotation.concurrent.ThreadSafe;

/**
 * File store backed by RocksDB.
 */
@ThreadSafe
public class RocksInodeStore implements InodeStore {
  private static final Logger LOG = LoggerFactory.getLogger(RocksInodeStore.class);
  private static final String INODES_DB_NAME = "inodes";
  private static final String INODES_COLUMN = "inodes";
  private static final String EDGES_COLUMN = "edges";
  private static final String ROCKS_STORE_NAME = "InodeStore";

  /*
   * Below 3 fields are created and managed by the external user class,
   * no need to close in this class.
   */
  // These are fields instead of constants because they depend on the call to RocksDB.loadLibrary().
  private final WriteOptions mDisableWAL;
  private final ReadOptions mReadPrefixSameAsStart;
  private final ReadOptions mIteratorOption;

  private final RocksStore mRocksStore;
  private final ColumnFamilyOptions mColumnFamilyOpts;

  /*
   * The ColumnFamilyHandle instances are created and closed in RocksStore
   */
  private final AtomicReference<ColumnFamilyHandle> mInodesColumn = new AtomicReference<>();
  private final AtomicReference<ColumnFamilyHandle> mEdgesColumn = new AtomicReference<>();

  /**
   * Creates and initializes a rocks block store.
   *
   * @param baseDir the base directory in which to store inode metadata
   */
  public RocksInodeStore(String baseDir) {
    RocksDB.loadLibrary();
    mDisableWAL = new WriteOptions().setDisableWAL(true);
    mToClose.add(mDisableWAL);
    mReadPrefixSameAsStart = new ReadOptions().setPrefixSameAsStart(true);
    mToClose.add(mReadPrefixSameAsStart);
    mIteratorOption = new ReadOptions().setReadaheadSize(
<<<<<<< HEAD
        ServerConfiguration.getBytes(PropertyKey.MASTER_METASTORE_ITERATOR_READAHEAD_SIZE));
||||||| parent of 9f152c554b (Fix Rocksdb thread safety using refcount(no lock))
        Configuration.getBytes(PropertyKey.MASTER_METASTORE_ITERATOR_READAHEAD_SIZE))
        .setTotalOrderSeek(true);
=======
        Configuration.getBytes(PropertyKey.MASTER_METASTORE_ITERATOR_READAHEAD_SIZE))
        .setTotalOrderSeek(true);
    mToClose.add(mIteratorOption);
>>>>>>> 9f152c554b (Fix Rocksdb thread safety using refcount(no lock))
    String dbPath = PathUtils.concatPath(baseDir, INODES_DB_NAME);
    String backupPath = PathUtils.concatPath(baseDir, INODES_DB_NAME + "-backup");
    mColumnFamilyOpts = new ColumnFamilyOptions()
        .setMemTableConfig(new HashLinkedListMemTableConfig())
        .setCompressionType(CompressionType.NO_COMPRESSION)
        .useFixedLengthPrefixExtractor(Longs.BYTES); // We always search using the initial long key
    List<ColumnFamilyDescriptor> columns = Arrays.asList(
        new ColumnFamilyDescriptor(INODES_COLUMN.getBytes(), mColumnFamilyOpts),
        new ColumnFamilyDescriptor(EDGES_COLUMN.getBytes(), mColumnFamilyOpts));
    mRocksStore = new RocksStore(ROCKS_STORE_NAME, dbPath, backupPath, columns,
        Arrays.asList(mInodesColumn, mEdgesColumn));

    // metrics
    final long CACHED_GAUGE_TIMEOUT_S =
        ServerConfiguration.getMs(PropertyKey.MASTER_METASTORE_METRICS_REFRESH_INTERVAL);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_BACKGROUND_ERRORS.getName(),
        () -> getProperty("rocksdb.background-errors"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_BLOCK_CACHE_CAPACITY.getName(),
        () -> getProperty("rocksdb.block-cache-capacity"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_BLOCK_CACHE_PINNED_USAGE.getName(),
        () -> getProperty("rocksdb.block-cache-pinned-usage"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_BLOCK_CACHE_USAGE.getName(),
        () -> getProperty("rocksdb.block-cache-usage"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_COMPACTION_PENDING.getName(),
        () -> getProperty("rocksdb.compaction-pending"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_CUR_SIZE_ACTIVE_MEM_TABLE.getName(),
        () -> getProperty("rocksdb.cur-size-active-mem-table"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_CUR_SIZE_ALL_MEM_TABLES.getName(),
        () -> getProperty("rocksdb.cur-size-all-mem-tables"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_ESTIMATE_NUM_KEYS.getName(),
        () -> getProperty("rocksdb.estimate-num-keys"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_ESTIMATE_PENDING_COMPACTION_BYTES.getName(),
        () -> getProperty("rocksdb.estimate-pending-compaction-bytes"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_ESTIMATE_TABLE_READERS_MEM.getName(),
        () -> getProperty("rocksdb.estimate-table-readers-mem"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_LIVE_SST_FILES_SIZE.getName(),
        () -> getProperty("rocksdb.live-sst-files-size"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_MEM_TABLE_FLUSH_PENDING.getName(),
        () -> getProperty("rocksdb.mem-table-flush-pending"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_NUM_DELETES_ACTIVE_MEM_TABLE.getName(),
        () -> getProperty("rocksdb.num-deletes-active-mem-table"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_NUM_DELETES_IMM_MEM_TABLES.getName(),
        () -> getProperty("rocksdb.num-deletes-imm-mem-tables"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_NUM_ENTRIES_ACTIVE_MEM_TABLE.getName(),
        () -> getProperty("rocksdb.num-entries-active-mem-table"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_NUM_ENTRIES_IMM_MEM_TABLES.getName(),
        () -> getProperty("rocksdb.num-entries-imm-mem-tables"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_NUM_IMMUTABLE_MEM_TABLE.getName(),
        () -> getProperty("rocksdb.num-immutable-mem-table"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_NUM_LIVE_VERSIONS.getName(),
        () -> getProperty("rocksdb.num-live-versions"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_NUM_RUNNING_COMPACTIONS.getName(),
        () -> getProperty("rocksdb.num-running-compactions"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_NUM_RUNNING_FLUSHES.getName(),
        () -> getProperty("rocksdb.num-running-flushes"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_SIZE_ALL_MEM_TABLES.getName(),
        () -> getProperty("rocksdb.size-all-mem-tables"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_TOTAL_SST_FILES_SIZE.getName(),
        () -> getProperty("rocksdb.total-sst-files-size"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);

    ImmutableSet<MetricKey> s = ImmutableSet.of(MetricKey.MASTER_ROCKS_INODE_BLOCK_CACHE_USAGE,
        MetricKey.MASTER_ROCKS_INODE_ESTIMATE_TABLE_READERS_MEM,
        MetricKey.MASTER_ROCKS_INODE_CUR_SIZE_ALL_MEM_TABLES,
        MetricKey.MASTER_ROCKS_INODE_BLOCK_CACHE_PINNED_USAGE);
    MetricsSystem.registerAggregatedCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_INODE_ESTIMATED_MEM_USAGE.getName(),
        s, CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);

    ImmutableSet<MetricKey> s1 = ImmutableSet.of(MetricKey.MASTER_ROCKS_BLOCK_ESTIMATED_MEM_USAGE,
        MetricKey.MASTER_ROCKS_INODE_ESTIMATED_MEM_USAGE);
    MetricsSystem.registerAggregatedCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_TOTAL_ESTIMATED_MEM_USAGE.getName(),
        s1, CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
  }

  private long getProperty(String rocksPropertyName) {
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
      return db().getAggregatedLongProperty(rocksPropertyName);
    } catch (RocksDBException e) {
      LOG.warn(String.format("error collecting %s", rocksPropertyName), e);
    }
    return -1;
  }

  @Override
  public void remove(Long inodeId) {
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
      byte[] id = Longs.toByteArray(inodeId);
      db().delete(mInodesColumn.get(), mDisableWAL, id);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeInode(MutableInode<?> inode) {
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
      db().put(mInodesColumn.get(), mDisableWAL, Longs.toByteArray(inode.getId()),
          inode.toProto().toByteArray());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public WriteBatch createWriteBatch() {
    return new RocksWriteBatch();
  }

  @Override
  public void clear() {
    LOG.info("Waiting to clear RocksInodeStore..");
    try (RocksExclusiveLockHandle lock = mRocksStore.lockForRewrite()) {
      LOG.info("Clearing RocksDB");
      mRocksStore.clear();
    }
    // Reset the DB state and prepare to serve again
    LOG.info("RocksInodeStore cleared and ready to serve again");
  }

  @Override
  public void addChild(long parentId, String childName, Long childId) {
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
      db().put(mEdgesColumn.get(), mDisableWAL, RocksUtils.toByteArray(parentId, childName),
          Longs.toByteArray(childId));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeChild(long parentId, String name) {
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
      db().delete(mEdgesColumn.get(), mDisableWAL, RocksUtils.toByteArray(parentId, name));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<MutableInode<?>> getMutable(long id, ReadOption option) {
    byte[] inode;
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
      inode = db().get(mInodesColumn.get(), Longs.toByteArray(id));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    if (inode == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(MutableInode.fromProto(InodeMeta.Inode.parseFrom(inode)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
<<<<<<< HEAD
  public Iterable<Long> getChildIds(Long inodeId, ReadOption option) {
    List<Long> ids = new ArrayList<>();
    try (RocksIterator iter = db().newIterator(mEdgesColumn.get(), mReadPrefixSameAsStart)) {
      iter.seek(Longs.toByteArray(inodeId));
      while (iter.isValid()) {
        ids.add(Longs.fromByteArray(iter.value()));
        iter.next();
||||||| parent of 9f152c554b (Fix Rocksdb thread safety using refcount(no lock))
  public CloseableIterator<Long> getChildIds(Long inodeId, ReadOption option) {
    RocksIterator iter = db().newIterator(mEdgesColumn.get(), mReadPrefixSameAsStart);
    // first seek to the correct bucket
    iter.seek(Longs.toByteArray(inodeId));
    // now seek to a specific file if needed
    String prefix = option.getPrefix();
    String fromName = option.getStartFrom();
    String seekTo;
    if (fromName != null && prefix != null) {
      if (fromName.compareTo(prefix) > 0) {
        seekTo = fromName;
      } else {
        seekTo = prefix;
=======
  public CloseableIterator<Long> getChildIds(Long inodeId, ReadOption option) {
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
      RocksIterator iter = db().newIterator(mEdgesColumn.get(), mReadPrefixSameAsStart);
      // first seek to the correct bucket
      iter.seek(Longs.toByteArray(inodeId));
      // now seek to a specific file if needed
      String prefix = option.getPrefix();
      String fromName = option.getStartFrom();
      String seekTo;
      if (fromName != null && prefix != null) {
        if (fromName.compareTo(prefix) > 0) {
          seekTo = fromName;
        } else {
          seekTo = prefix;
        }
      } else if (fromName != null) {
        seekTo = fromName;
      } else {
        seekTo = prefix;
>>>>>>> 9f152c554b (Fix Rocksdb thread safety using refcount(no lock))
      }
<<<<<<< HEAD
    }
    return ids;
||||||| parent of 9f152c554b (Fix Rocksdb thread safety using refcount(no lock))
    } else if (fromName != null) {
      seekTo = fromName;
    } else {
      seekTo = prefix;
    }
    if (seekTo != null && seekTo.length() > 0) {
      iter.seek(RocksUtils.toByteArray(inodeId, seekTo));
    }
    RocksIter rocksIter = new RocksIter(iter, prefix);
    Stream<Long> idStream = StreamSupport.stream(Spliterators
        .spliteratorUnknownSize(rocksIter, Spliterator.ORDERED), false);
    return CloseableIterator.create(idStream.iterator(), (any) -> iter.close());
=======
      if (seekTo != null && seekTo.length() > 0) {
        iter.seek(RocksUtils.toByteArray(inodeId, seekTo));
      }
      /*
       * Acquire a second lock for iteration, instead of using the same lock for initialization.
       * Because init takes many operations and should be protected by try-with-resource.
       * This is fine because the shared lock is reentrant.
       */
      RocksSharedLockHandle readLock = mRocksStore.checkAndAcquireSharedLock();
      RocksIter rocksIter = new RocksIter(iter, prefix, () -> {
        mRocksStore.shouldAbort(readLock.getLockVersion());
        return null;
      });
      Stream<Long> idStream = StreamSupport.stream(Spliterators
          .spliteratorUnknownSize(rocksIter, Spliterator.ORDERED), false);
      return CloseableIterator.create(idStream.iterator(), (any) -> {
        try {
          iter.close();
        } finally {
          readLock.close();
        }
      });
    }
>>>>>>> 9f152c554b (Fix Rocksdb thread safety using refcount(no lock))
  }

  @Override
  public Optional<Long> getChildId(Long inodeId, String name, ReadOption option) {
    byte[] id;
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
      id = db().get(mEdgesColumn.get(), RocksUtils.toByteArray(inodeId, name));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    if (id == null) {
      return Optional.empty();
    }
    return Optional.of(Longs.fromByteArray(id));
  }

<<<<<<< HEAD
||||||| parent of 9f152c554b (Fix Rocksdb thread safety using refcount(no lock))
  static class RocksIter implements Iterator<Long> {

    final RocksIterator mIter;
    boolean mStopped = false;
    final byte[] mPrefix;

    RocksIter(RocksIterator rocksIterator, @Nullable String prefix) {
      mIter = rocksIterator;
      if (prefix != null && prefix.length() > 0) {
        mPrefix = prefix.getBytes();
      } else {
        mPrefix = null;
      }
      checkPrefix();
    }

    private void checkPrefix() {
      if (mIter.isValid() && mPrefix != null) {
        byte[] key = mIter.key();
        if (key.length + Longs.BYTES < mPrefix.length) {
          mStopped = true;
          return;
        }
        for (int i = 0; i < mPrefix.length; i++) {
          if (mPrefix[i] != key[i + Longs.BYTES]) {
            mStopped = true;
            return;
          }
        }
      }
    }

    @Override
    public boolean hasNext() {
      return mIter.isValid() && !mStopped;
    }

    @Override
    public Long next() {
      Long l = Longs.fromByteArray(mIter.value());
      mIter.next();
      checkPrefix();
      return l;
    }
  }

=======
  static class RocksIter implements Iterator<Long> {

    final RocksIterator mIter;
    boolean mStopped = false;
    final byte[] mPrefix;
    Supplier<Void> mAbortCheck;

    RocksIter(RocksIterator rocksIterator, @Nullable String prefix,
          Supplier<Void> abortCheck) {
      mIter = rocksIterator;
      if (prefix != null && prefix.length() > 0) {
        mPrefix = prefix.getBytes();
      } else {
        mPrefix = null;
      }
      checkPrefix();
      mAbortCheck = abortCheck;
    }

    private void checkPrefix() {
      if (mIter.isValid() && mPrefix != null) {
        byte[] key = mIter.key();
        if (key.length + Longs.BYTES < mPrefix.length) {
          mStopped = true;
          return;
        }
        for (int i = 0; i < mPrefix.length; i++) {
          if (mPrefix[i] != key[i + Longs.BYTES]) {
            mStopped = true;
            return;
          }
        }
      }
    }

    @Override
    public boolean hasNext() {
      return mIter.isValid() && !mStopped;
    }

    @Override
    public Long next() {
      // Abort the operation if RocksDB stops serving
      mAbortCheck.get();
      Long l = Longs.fromByteArray(mIter.value());
      mIter.next();
      checkPrefix();
      return l;
    }
  }

>>>>>>> 9f152c554b (Fix Rocksdb thread safety using refcount(no lock))
  @Override
  public Optional<Inode> getChild(Long inodeId, String name, ReadOption option) {
    // The underlying calls should each handle locking internally
    return getChildId(inodeId, name).flatMap(id -> {
      Optional<Inode> child = get(id);
      if (!child.isPresent()) {
        LOG.warn("Found child edge {}->{}={}, but inode {} does not exist", inodeId, name,
            id, id);
      }
      return child;
    });
  }

  @Override
  public boolean hasChildren(InodeDirectoryView inode, ReadOption option) {
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock();
         RocksIterator iter = db().newIterator(mEdgesColumn.get(), mReadPrefixSameAsStart)) {
      iter.seek(Longs.toByteArray(inode.getId()));
      return iter.isValid();
    }
  }

  @Override
  public Set<EdgeEntry> allEdges() {
    Set<EdgeEntry> edges = new HashSet<>();
<<<<<<< HEAD
    try (RocksIterator iter = db().newIterator(mEdgesColumn.get())) {
||||||| parent of 9f152c554b (Fix Rocksdb thread safety using refcount(no lock))
    try (RocksIterator iter = db().newIterator(mEdgesColumn.get(),
        mIteratorOption)) {
=======
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock();
         RocksIterator iter = db().newIterator(mEdgesColumn.get(), mIteratorOption)) {
>>>>>>> 9f152c554b (Fix Rocksdb thread safety using refcount(no lock))
      iter.seekToFirst();
      while (iter.isValid()) {
        mRocksStore.shouldAbort(lock.getLockVersion());
        long parentId = RocksUtils.readLong(iter.key(), 0);
        String childName = new String(iter.key(), Longs.BYTES, iter.key().length - Longs.BYTES);
        long childId = Longs.fromByteArray(iter.value());
        edges.add(new EdgeEntry(parentId, childName, childId));
        iter.next();
      }
    }
    return edges;
  }

  @Override
  public Set<MutableInode<?>> allInodes() {
    Set<MutableInode<?>> inodes = new HashSet<>();
<<<<<<< HEAD
    try (RocksIterator iter = db().newIterator(mInodesColumn.get())) {
||||||| parent of 9f152c554b (Fix Rocksdb thread safety using refcount(no lock))
    try (RocksIterator iter = db().newIterator(mInodesColumn.get(),
        mIteratorOption)) {
=======
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock();
         RocksIterator iter = db().newIterator(mInodesColumn.get(), mIteratorOption)) {
>>>>>>> 9f152c554b (Fix Rocksdb thread safety using refcount(no lock))
      iter.seekToFirst();
      while (iter.isValid()) {
        mRocksStore.shouldAbort(lock.getLockVersion());
        inodes.add(getMutable(Longs.fromByteArray(iter.key()), ReadOption.defaults()).get());
        iter.next();
      }
    }
    return inodes;
  }

  /**
   * Acquires an iterator to iterate all Inodes in RocksDB.
   * A shared lock will be acquired when this iterator is created, and released when:
   * 1. This iterator is complete.
   * 2. At each step, the iterator finds the RocksDB is closing and aborts voluntarily.
   *
   * Except tests, this iterator is only used in:
   * 1. {@link alluxio.master.journal.tool.AbstractJournalDumper} which translates RocksDB
   *    checkpoints to a human-readable form.
   *
   * @return an iterator over stored inodes
   */
  public CloseableIterator<InodeView> getCloseableIterator() {
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
      RocksSharedLockHandle readLock = mRocksStore.checkAndAcquireSharedLock();
      return RocksUtils.createCloseableIterator(
          db().newIterator(mInodesColumn.get(), mIteratorOption),
          (iter) -> getMutable(Longs.fromByteArray(iter.key()), ReadOption.defaults()).get(),
          () -> {
            mRocksStore.shouldAbort(lock.getLockVersion());
            return null;
          }, readLock);
    }
  }

  @Override
  public boolean supportsBatchWrite() {
    return true;
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.ROCKS_INODE_STORE;
  }

  @Override
  public void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException {
    mRocksStore.writeToCheckpoint(output);
  }

  @Override
  public void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    mRocksStore.restoreFromCheckpoint(input);
  }

  private class RocksWriteBatch implements WriteBatch {
    private final org.rocksdb.WriteBatch mBatch = new org.rocksdb.WriteBatch();

    @Override
    public void writeInode(MutableInode<?> inode) {
      try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
        mBatch.put(mInodesColumn.get(), Longs.toByteArray(inode.getId()),
            inode.toProto().toByteArray());
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void removeInode(Long key) {
      try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
        mBatch.delete(mInodesColumn.get(), Longs.toByteArray(key));
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void addChild(Long parentId, String childName, Long childId) {
      try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
        mBatch.put(mEdgesColumn.get(), RocksUtils.toByteArray(parentId, childName),
            Longs.toByteArray(childId));
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void removeChild(Long parentId, String childName) {
      try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
        mBatch.delete(mEdgesColumn.get(), RocksUtils.toByteArray(parentId, childName));
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void commit() {
      try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
        db().write(mDisableWAL, mBatch);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      mBatch.close();
    }
  }

  @Override
  public void close() {
<<<<<<< HEAD
    LOG.info("Closing RocksInodeStore and recycling all RocksDB JNI objects");
    mRocksStore.close();
    mColumnFamilyOpts.close();
    LOG.info("RocksInodeStore closed");
||||||| parent of 9f152c554b (Fix Rocksdb thread safety using refcount(no lock))
    LOG.info("Closing RocksInodeStore and recycling all RocksDB JNI objects");
    mRocksStore.close();
    mDisableWAL.close();
    mReadPrefixSameAsStart.close();
    // Close the elements in the reverse order they were added
    Collections.reverse(mToClose);
    mToClose.forEach(RocksObject::close);
    LOG.info("RocksInodeStore closed");
=======
    LOG.info("RocksInodeStore is being closed");
    try (RocksExclusiveLockHandle lock = mRocksStore.lockForClosing()) {
      mRocksStore.close();
      // Close the elements in the reverse order they were added
      Collections.reverse(mToClose);
      mToClose.forEach(RocksObject::close);
    }
>>>>>>> 9f152c554b (Fix Rocksdb thread safety using refcount(no lock))
  }

  private RocksDB db() {
    return mRocksStore.getDb();
  }

  /**
   * @return a newline-delimited string representing the state of the inode store. This is useful
   *         for debugging purposes
   */
  public String toStringEntries() {
    StringBuilder sb = new StringBuilder();
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock();
         ReadOptions readOptions = new ReadOptions().setTotalOrderSeek(true);
         RocksIterator inodeIter = db().newIterator(mInodesColumn.get(), readOptions)) {
      inodeIter.seekToFirst();
      while (inodeIter.isValid()) {
        mRocksStore.shouldAbort(lock.getLockVersion());
        MutableInode<?> inode;
        try {
          inode = MutableInode.fromProto(InodeMeta.Inode.parseFrom(inodeIter.value()));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        sb.append("Inode " + Longs.fromByteArray(inodeIter.key()) + ": " + inode + "\n");
        inodeIter.next();
      }
    }
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock();
         RocksIterator edgeIter = db().newIterator(mEdgesColumn.get())) {
      edgeIter.seekToFirst();
      while (edgeIter.isValid()) {
        mRocksStore.shouldAbort(lock.getLockVersion());
        byte[] key = edgeIter.key();
        byte[] id = new byte[Longs.BYTES];
        byte[] name = new byte[key.length - Longs.BYTES];
        System.arraycopy(key, 0, id, 0, Longs.BYTES);
        System.arraycopy(key, Longs.BYTES, name, 0, key.length - Longs.BYTES);
        sb.append(String.format("<%s,%s>->%s%n", Longs.fromByteArray(id), new String(name),
            Longs.fromByteArray(edgeIter.value())));
        edgeIter.next();
      }
    }
    return sb.toString();
  }
<<<<<<< HEAD
||||||| parent of 9f152c554b (Fix Rocksdb thread safety using refcount(no lock))

  /**
   * A testing only method to access the internal objects.
   * @return the RocksDB objects references the InodesColumn
   */
  @VisibleForTesting
  public Pair<RocksDB, AtomicReference<ColumnFamilyHandle>> getDBInodeColumn() {
    return new Pair<>(db(), mInodesColumn);
  }
=======

  /**
   * A testing only method to access the internal objects.
   * For simplicity, no thread safety is provided on the escaping objects.
   *
   * @return the RocksDB objects references the InodesColumn
   */
  @VisibleForTesting
  public Pair<RocksDB, AtomicReference<ColumnFamilyHandle>> getDBInodeColumn() {
    return new Pair<>(db(), mInodesColumn);
  }
>>>>>>> 9f152c554b (Fix Rocksdb thread safety using refcount(no lock))
}
