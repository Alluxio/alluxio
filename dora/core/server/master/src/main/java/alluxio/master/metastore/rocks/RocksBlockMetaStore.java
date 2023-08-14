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

import static alluxio.rocks.RocksStore.checkSetTableConfig;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.metastore.BlockMetaStore;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.meta.Block.BlockLocation;
import alluxio.proto.meta.Block.BlockMeta;
import alluxio.resource.CloseableIterator;
import alluxio.rocks.RocksCheckpointed;
import alluxio.rocks.RocksStore;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.HashLinkedListMemTableConfig;
import org.rocksdb.OptionsUtil;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksObject;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Block store backed by RocksDB.
 */
@ThreadSafe
public class RocksBlockMetaStore implements BlockMetaStore, RocksCheckpointed {
  private static final Logger LOG = LoggerFactory.getLogger(RocksBlockMetaStore.class);
  private static final String BLOCKS_DB_NAME = "blocks";
  private static final String BLOCK_META_COLUMN = "block-meta";
  private static final String BLOCK_LOCATIONS_COLUMN = "block-locations";
  private static final String ROCKS_STORE_NAME = "BlockStore";

  /*
   * Below 3 fields are created and managed by the external user class,
   * no need to close in this class
   */
  // This is a field instead of a constant because it depends on the call to RocksDB.loadLibrary().
  private final WriteOptions mDisableWAL;
  private final ReadOptions mIteratorOption;
  private final ReadOptions mReadPrefixSameAsStart;

  private final List<RocksObject> mToClose = new ArrayList<>();

  private final RocksStore mRocksStore;
  /*
   * The ColumnFamilyHandle instances are created and closed in RocksStore
   */
  private final AtomicReference<ColumnFamilyHandle> mBlockMetaColumn = new AtomicReference<>();
  private final AtomicReference<ColumnFamilyHandle> mBlockLocationsColumn = new AtomicReference<>();
  private final LongAdder mSize = new LongAdder();

  /**
   * Creates and initializes a rocks block store.
   *
   * @param baseDir the base directory in which to store block store metadata
   */
  public RocksBlockMetaStore(String baseDir) {
    RocksDB.loadLibrary();
    // the rocksDB objects must be initialized after RocksDB.loadLibrary() is called
    mDisableWAL = new WriteOptions().setDisableWAL(true);
    mToClose.add(mDisableWAL);
    mReadPrefixSameAsStart = new ReadOptions().setPrefixSameAsStart(true);
    mToClose.add(mReadPrefixSameAsStart);
    mIteratorOption = new ReadOptions()
        .setReadaheadSize(Configuration.getBytes(
            PropertyKey.MASTER_METASTORE_ITERATOR_READAHEAD_SIZE))
        .setTotalOrderSeek(true);
    mToClose.add(mIteratorOption);

    List<ColumnFamilyDescriptor> columns = new ArrayList<>();
    DBOptions opts = new DBOptions();
    mToClose.add(opts);
    if (Configuration.isSet(PropertyKey.ROCKS_BLOCK_CONF_FILE)) {
      try {
        String confPath = Configuration.getString(PropertyKey.ROCKS_BLOCK_CONF_FILE);
        LOG.info("Opening RocksDB Block table configuration file {}", confPath);
        OptionsUtil.loadOptionsFromFile(confPath, Env.getDefault(), opts, columns, false);
      } catch (RocksDBException e) {
        throw new IllegalArgumentException(e);
      }
      Preconditions.checkArgument(columns.size() == 3
              && new String(columns.get(1).getName()).equals(BLOCK_META_COLUMN)
              && new String(columns.get(2).getName()).equals(BLOCK_LOCATIONS_COLUMN),
          String.format("Invalid RocksDB block store table configuration,"
                  + " expected 3 columns, default, %s, and %s",
              BLOCK_META_COLUMN, BLOCK_LOCATIONS_COLUMN));
      // Remove the default column as it is created in RocksStore
      columns.remove(0).getOptions().close();
    } else {
      opts.setAllowConcurrentMemtableWrite(false) // not supported for hash mem tables
          .setCreateMissingColumnFamilies(true)
          .setCreateIfMissing(true)
          .setMaxOpenFiles(-1);
      // This is a field instead of a constant as it depends on the call to RocksDB.loadLibrary().
      CompressionType compressionType =
          Configuration.getEnum(PropertyKey.MASTER_METASTORE_ROCKS_CHECKPOINT_COMPRESSION_TYPE,
              CompressionType.class);
      columns.add(new ColumnFamilyDescriptor(BLOCK_META_COLUMN.getBytes(),
          new ColumnFamilyOptions()
              .useFixedLengthPrefixExtractor(Longs.BYTES) // allows memtable buckets by block id
              .setMemTableConfig(new HashLinkedListMemTableConfig()) // bucket contains single value
              .setCompressionType(compressionType)));
      columns.add(new ColumnFamilyDescriptor(BLOCK_LOCATIONS_COLUMN.getBytes(),
          new ColumnFamilyOptions()
              .useFixedLengthPrefixExtractor(Longs.BYTES) // allows memtable buckets by block id
              .setMemTableConfig(new HashLinkedListMemTableConfig()) // bucket contains worker info
              .setCompressionType(compressionType)));
    }

    mToClose.addAll(columns.stream().map(
        ColumnFamilyDescriptor::getOptions).collect(Collectors.toList()));

    // The following options are set by property keys as they are not able to be
    // set using configuration files.
    checkSetTableConfig(PropertyKey.MASTER_METASTORE_ROCKS_BLOCK_META_CACHE_SIZE,
        PropertyKey.MASTER_METASTORE_ROCKS_BLOCK_META_BLOOM_FILTER,
        PropertyKey.MASTER_METASTORE_ROCKS_BLOCK_META_INDEX,
        PropertyKey.MASTER_METASTORE_ROCKS_BLOCK_META_BLOCK_INDEX, mToClose)
        .ifPresent(cfg -> columns.get(0).getOptions().setTableFormatConfig(cfg));
    checkSetTableConfig(PropertyKey.MASTER_METASTORE_ROCKS_BLOCK_LOCATION_CACHE_SIZE,
        PropertyKey.MASTER_METASTORE_ROCKS_BLOCK_LOCATION_BLOOM_FILTER,
        PropertyKey.MASTER_METASTORE_ROCKS_BLOCK_LOCATION_INDEX,
        PropertyKey.MASTER_METASTORE_ROCKS_BLOCK_LOCATION_BLOCK_INDEX, mToClose)
        .ifPresent(cfg -> columns.get(1).getOptions().setTableFormatConfig(cfg));
    String dbPath = PathUtils.concatPath(baseDir, BLOCKS_DB_NAME);
    String backupPath = PathUtils.concatPath(baseDir, BLOCKS_DB_NAME + "-backups");
    // Create block store db path if it does not exist.
    if (!FileUtils.exists(dbPath)) {
      try {
        FileUtils.createDir(dbPath);
      } catch (IOException e) {
        LOG.warn("Failed to create nonexistent db path at: {}. Error:{}", dbPath, e);
      }
    }
    mRocksStore = new RocksStore(ROCKS_STORE_NAME, dbPath, backupPath, opts, columns,
        Arrays.asList(mBlockMetaColumn, mBlockLocationsColumn), true);

    // metrics
    final long CACHED_GAUGE_TIMEOUT_S =
        Configuration.getMs(PropertyKey.MASTER_METASTORE_METRICS_REFRESH_INTERVAL);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_BACKGROUND_ERRORS.getName(),
        () -> getProperty("rocksdb.background-errors"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_BLOCK_CACHE_CAPACITY.getName(),
        () -> getProperty("rocksdb.block-cache-capacity"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_BLOCK_CACHE_PINNED_USAGE.getName(),
        () -> getProperty("rocksdb.block-cache-pinned-usage"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_BLOCK_CACHE_USAGE.getName(),
        () -> getProperty("rocksdb.block-cache-usage"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_COMPACTION_PENDING.getName(),
        () -> getProperty("rocksdb.compaction-pending"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_CUR_SIZE_ACTIVE_MEM_TABLE.getName(),
        () -> getProperty("rocksdb.cur-size-active-mem-table"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_CUR_SIZE_ALL_MEM_TABLES.getName(),
        () -> getProperty("rocksdb.cur-size-all-mem-tables"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_ESTIMATE_NUM_KEYS.getName(),
        () -> getProperty("rocksdb.estimate-num-keys"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_ESTIMATE_PENDING_COMPACTION_BYTES.getName(),
        () -> getProperty("rocksdb.estimate-pending-compaction-bytes"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_ESTIMATE_TABLE_READERS_MEM.getName(),
        () -> getProperty("rocksdb.estimate-table-readers-mem"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_LIVE_SST_FILES_SIZE.getName(),
        () -> getProperty("rocksdb.live-sst-files-size"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_MEM_TABLE_FLUSH_PENDING.getName(),
        () -> getProperty("rocksdb.mem-table-flush-pending"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_NUM_DELETES_ACTIVE_MEM_TABLE.getName(),
        () -> getProperty("rocksdb.num-deletes-active-mem-table"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_NUM_DELETES_IMM_MEM_TABLES.getName(),
        () -> getProperty("rocksdb.num-deletes-imm-mem-tables"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_NUM_ENTRIES_ACTIVE_MEM_TABLE.getName(),
        () -> getProperty("rocksdb.num-entries-active-mem-table"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_NUM_ENTRIES_IMM_MEM_TABLES.getName(),
        () -> getProperty("rocksdb.num-entries-imm-mem-tables"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_NUM_IMMUTABLE_MEM_TABLE.getName(),
        () -> getProperty("rocksdb.num-immutable-mem-table"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_NUM_LIVE_VERSIONS.getName(),
        () -> getProperty("rocksdb.num-live-versions"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_NUM_RUNNING_COMPACTIONS.getName(),
        () -> getProperty("rocksdb.num-running-compactions"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_NUM_RUNNING_FLUSHES.getName(),
        () -> getProperty("rocksdb.num-running-flushes"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_SIZE_ALL_MEM_TABLES.getName(),
        () -> getProperty("rocksdb.size-all-mem-tables"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_TOTAL_SST_FILES_SIZE.getName(),
        () -> getProperty("rocksdb.total-sst-files-size"),
        CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);

    ImmutableSet<MetricKey> s = ImmutableSet.of(MetricKey.MASTER_ROCKS_BLOCK_BLOCK_CACHE_USAGE,
        MetricKey.MASTER_ROCKS_BLOCK_ESTIMATE_TABLE_READERS_MEM,
        MetricKey.MASTER_ROCKS_BLOCK_CUR_SIZE_ALL_MEM_TABLES,
        MetricKey.MASTER_ROCKS_BLOCK_BLOCK_CACHE_PINNED_USAGE);
    MetricsSystem.registerAggregatedCachedGaugeIfAbsent(
        MetricKey.MASTER_ROCKS_BLOCK_ESTIMATED_MEM_USAGE.getName(),
        s, CACHED_GAUGE_TIMEOUT_S, TimeUnit.MILLISECONDS);
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
  public Optional<BlockMeta> getBlock(long id) {
    byte[] meta;
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
      meta = db().get(mBlockMetaColumn.get(), Longs.toByteArray(id));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    if (meta == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(BlockMeta.parseFrom(meta));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void putBlock(long id, BlockMeta meta) {
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
      byte[] buf = db().get(mBlockMetaColumn.get(), Longs.toByteArray(id));
      // Overwrites the key if it already exists.
      db().put(mBlockMetaColumn.get(), mDisableWAL, Longs.toByteArray(id), meta.toByteArray());
      if (buf == null) {
        // key did not exist before
        mSize.increment();
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeBlock(long id) {
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
      byte[] buf = db().get(mBlockMetaColumn.get(), Longs.toByteArray(id));
      db().delete(mBlockMetaColumn.get(), mDisableWAL, Longs.toByteArray(id));
      if (buf != null) {
        // Key existed before
        mSize.decrement();
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() {
    LOG.info("Waiting to clear RocksBlockMetaStore..");
    try (RocksExclusiveLockHandle lock = mRocksStore.lockForRewrite()) {
      LOG.info("Clearing RocksDB");
      mSize.reset();
      mRocksStore.clear();
    }
    // Reset the DB state and prepare to serve again
    LOG.info("RocksBlockMetaStore cleared and ready to serve again");
  }

  @Override
  public long size() {
    return mSize.longValue();
  }

  @Override
  /**
   * There may be concurrent readers and writers so we have to guarantee thread safety when
   * closing the RocksDB and all RocksObject instances. The sequence for closing is:
   * 1. Mark flag mClosed = true without locking.
   *    All new readers/writers should see the flag and not start the operation.
   * 2. Acquire the WriteLock before shutting down, so it waits for all concurrent r/w to
   *    bail or finish.
   */
  public void close() {
    LOG.info("RocksBlockStore is being closed");
    try (RocksExclusiveLockHandle lock = mRocksStore.lockForClosing()) {
      mSize.reset();
      mRocksStore.close();
      // Close the elements in the reverse order they were added
      Collections.reverse(mToClose);
      mToClose.forEach(RocksObject::close);
    }
  }

  @Override
  public List<BlockLocation> getLocations(long id) {
    // References to the RocksObject need to be held explicitly and kept from GC
    // In order to prevent segfaults in the native code execution
    // Ref: https://github.com/facebook/rocksdb/issues/9378
    // All RocksObject should be closed properly at the end of usage
    // When there are multiple resources declared in the try-with-resource block
    // They are closed in the opposite order of declaration
    // Ref: https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
    // We assume this operation is short (one block cannot have too many locations)
    // and lock the full iteration
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock();
         final RocksIterator iter = db().newIterator(mBlockLocationsColumn.get(),
            mReadPrefixSameAsStart)) {
      iter.seek(Longs.toByteArray(id));
      List<BlockLocation> locations = new ArrayList<>();
      for (; iter.isValid(); iter.next()) {
        try {
          locations.add(BlockLocation.parseFrom(iter.value()));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      return locations;
    }
  }

  @Override
  public void addLocation(long id, BlockLocation location) {
    byte[] key = RocksUtils.toByteArray(id, location.getWorkerId());
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
      db().put(mBlockLocationsColumn.get(), mDisableWAL, key, location.toByteArray());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeLocation(long blockId, long workerId) {
    byte[] key = RocksUtils.toByteArray(blockId, workerId);
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
      db().delete(mBlockLocationsColumn.get(), mDisableWAL, key);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  /**
   * Acquires an iterator to iterate all Blocks in RocksDB.
   * A shared lock will be acquired when this iterator is created, and released when:
   * 1. This iterator is complete.
   * 2. At each step, the iterator finds the RocksDB is closing and aborts voluntarily.
   *
   * This iterator is used in:
   * 1. Journal dumping like checkpoint/backup sequences
   */
  public CloseableIterator<Block> getCloseableIterator() {
    try (RocksSharedLockHandle lock = mRocksStore.checkAndAcquireSharedLock()) {
      RocksSharedLockHandle readLock = mRocksStore.checkAndAcquireSharedLock();

      RocksIterator iterator = db().newIterator(mBlockMetaColumn.get(), mIteratorOption);
      return RocksUtils.createCloseableIterator(iterator,
          (iter) -> new Block(Longs.fromByteArray(iter.key()), BlockMeta.parseFrom(iter.value())),
          () -> {
            mRocksStore.shouldAbort(lock.getLockVersion());
            return null;
          }, readLock);
    }
  }

  private RocksDB db() {
    return mRocksStore.getDb();
  }

  @Override
  public RocksStore getRocksStore() {
    return mRocksStore;
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.BLOCK_MASTER;
  }
}
