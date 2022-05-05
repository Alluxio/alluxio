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
import alluxio.master.metastore.BlockStore;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.meta.Block.BlockLocation;
import alluxio.proto.meta.Block.BlockMeta;
import alluxio.resource.CloseableIterator;
import alluxio.util.io.FileUtils;
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
import org.rocksdb.Slice;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Block store backed by RocksDB.
 */
@ThreadSafe
public class RocksBlockStore implements BlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(RocksBlockStore.class);
  private static final String BLOCKS_DB_NAME = "blocks";
  private static final String BLOCK_META_COLUMN = "block-meta";
  private static final String BLOCK_LOCATIONS_COLUMN = "block-locations";
  private static final String ROCKS_STORE_NAME = "BlockStore";

  // This is a field instead of a constant because it depends on the call to RocksDB.loadLibrary().
  private final WriteOptions mDisableWAL;
  private final ReadOptions mIteratorOption;
  private final ColumnFamilyOptions mColumnFamilyOptions;

  private final RocksStore mRocksStore;
  // The handles are closed in RocksStore
  private final AtomicReference<ColumnFamilyHandle> mBlockMetaColumn = new AtomicReference<>();
  private final AtomicReference<ColumnFamilyHandle> mBlockLocationsColumn = new AtomicReference<>();
  private final LongAdder mSize = new LongAdder();

  /**
   * Creates and initializes a rocks block store.
   *
   * @param baseDir the base directory in which to store block store metadata
   */
  public RocksBlockStore(String baseDir) {
    RocksDB.loadLibrary();
    mDisableWAL = new WriteOptions().setDisableWAL(true);
    mIteratorOption = new ReadOptions().setReadaheadSize(
        ServerConfiguration.getBytes(PropertyKey.MASTER_METASTORE_ITERATOR_READAHEAD_SIZE));
    mColumnFamilyOptions = new ColumnFamilyOptions()
        .setMemTableConfig(new HashLinkedListMemTableConfig())
        .setCompressionType(CompressionType.NO_COMPRESSION);
    List<ColumnFamilyDescriptor> columns = Arrays.asList(
        new ColumnFamilyDescriptor(BLOCK_META_COLUMN.getBytes(), mColumnFamilyOptions),
        new ColumnFamilyDescriptor(BLOCK_LOCATIONS_COLUMN.getBytes(), mColumnFamilyOptions));
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
    mRocksStore = new RocksStore(ROCKS_STORE_NAME, dbPath, backupPath, columns,
        Arrays.asList(mBlockMetaColumn, mBlockLocationsColumn));

    // metrics
    final long CACHED_GAUGE_TIMEOUT_S =
        ServerConfiguration.getMs(PropertyKey.MASTER_METASTORE_METRICS_REFRESH_INTERVAL);
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
    try {
      return db().getAggregatedLongProperty(rocksPropertyName);
    } catch (RocksDBException e) {
      LOG.warn(String.format("error collecting %s", rocksPropertyName), e);
    }
    return -1;
  }

  @Override
  public Optional<BlockMeta> getBlock(long id) {
    byte[] meta;
    try {
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
    try {
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
    try {
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
    mSize.reset();
    mRocksStore.clear();
  }

  @Override
  public long size() {
    return mSize.longValue();
  }

  @Override
  public void close() {
    mSize.reset();
    LOG.info("Closing RocksBlockStore and recycling all RocksDB JNI objects");
    mRocksStore.close();
    mIteratorOption.close();
    mDisableWAL.close();
    mColumnFamilyOptions.close();
    LOG.info("RocksBlockStore closed");
  }

  @Override
  public List<BlockLocation> getLocations(long id) {
    byte[] startKey = RocksUtils.toByteArray(id, 0);
    byte[] endKey = RocksUtils.toByteArray(id, Long.MAX_VALUE);

    // References to the RocksObject need to be held explicitly and kept from GC
    // In order to prevent segfaults in the native code execution
    // Ref: https://github.com/facebook/rocksdb/issues/9378
    // All RocksObject should be closed properly at the end of usage
    // When there are multiple resources declared in the try-with-resource block
    // They are closed in the opposite order of declaration
    // Ref: https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
    try (final Slice slice = new Slice(endKey);
         final ReadOptions readOptions = new ReadOptions().setIterateUpperBound(slice);
         final RocksIterator iter = db().newIterator(mBlockLocationsColumn.get(), readOptions)) {
      iter.seek(startKey);
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
    try {
      db().put(mBlockLocationsColumn.get(), mDisableWAL, key, location.toByteArray());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeLocation(long blockId, long workerId) {
    byte[] key = RocksUtils.toByteArray(blockId, workerId);
    try {
      db().delete(mBlockLocationsColumn.get(), mDisableWAL, key);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public CloseableIterator<Block> getCloseableIterator() {
    RocksIterator iterator = db().newIterator(mBlockMetaColumn.get(), mIteratorOption);
    return RocksUtils.createCloseableIterator(iterator,
        (iter) -> new Block(Longs.fromByteArray(iter.key()), BlockMeta.parseFrom(iter.value())));
  }

  private RocksDB db() {
    return mRocksStore.getDb();
  }
}
