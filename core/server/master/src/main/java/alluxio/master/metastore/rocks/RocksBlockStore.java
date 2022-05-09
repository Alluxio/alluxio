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
import alluxio.proto.meta.Block.BlockLocation;
import alluxio.proto.meta.Block.BlockMeta;
import alluxio.resource.CloseableIterator;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;

import com.google.common.primitives.Longs;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.DataBlockIndexType;
import org.rocksdb.Env;
import org.rocksdb.Filter;
import org.rocksdb.HashLinkedListMemTableConfig;
import org.rocksdb.HashSkipListMemTableConfig;
import org.rocksdb.IndexType;
import org.rocksdb.LRUCache;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
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
  private final ReadOptions mReadPrefixSameAsStart;

  private final List<RocksObject> mToClose = new ArrayList<>();

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
    mReadPrefixSameAsStart = new ReadOptions().setPrefixSameAsStart(true);
    mIteratorOption = new ReadOptions()
        .setReadaheadSize(ServerConfiguration.getBytes(
            PropertyKey.MASTER_METASTORE_ITERATOR_READAHEAD_SIZE))
        .setTotalOrderSeek(true);

    List<ColumnFamilyDescriptor> columns = new ArrayList<>();
    DBOptions opts = new DBOptions();
    if (ServerConfiguration.isSet(PropertyKey.ROCKS_BLOCK_CONF_FILE)) {
      try {
        OptionsUtil.loadOptionsFromFile(ServerConfiguration.getString(
            PropertyKey.ROCKS_BLOCK_CONF_FILE), Env.getDefault(), opts, columns,
            false);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
      if (columns.size() != 3
          || !new String(columns.get(1).getName()).equals(BLOCK_META_COLUMN)
          || !new String(columns.get(2).getName()).equals(BLOCK_LOCATIONS_COLUMN)) {
        throw new RuntimeException(String.format("Invalid RocksDB block store table configuration,"
            + "expected 3 columns, default, %s, and %s",
            BLOCK_META_COLUMN, BLOCK_LOCATIONS_COLUMN));
      }
      // Remove the default column as it is created in RocksStore
      columns.remove(0).getOptions().close();
    } else {
      opts = new DBOptions()
          .setAllowConcurrentMemtableWrite(false) // not supported for hash mem tables
          .setCreateMissingColumnFamilies(true)
          .setCreateIfMissing(true)
          .setMaxOpenFiles(-1);
      columns.add(new ColumnFamilyDescriptor(BLOCK_META_COLUMN.getBytes(),
          new ColumnFamilyOptions()
              .useFixedLengthPrefixExtractor(Longs.BYTES) // allows memtable buckets by block id
              .setMemTableConfig(new HashLinkedListMemTableConfig()) // bucket contains single value
              .setMemtablePrefixBloomSizeRatio(0.02) // for fast bucket memtable lookups
              .optimizeLevelStyleCompaction() // use level style compaction
              .setMemtableWholeKeyFiltering(true) // fast point lookups for bucket id
              .setCompressionType(CompressionType.NO_COMPRESSION)));
      columns.add(new ColumnFamilyDescriptor(BLOCK_LOCATIONS_COLUMN.getBytes(),
          new ColumnFamilyOptions()
              .useFixedLengthPrefixExtractor(Longs.BYTES) // allows memtable buckets by block id
              .setMemTableConfig(new HashSkipListMemTableConfig()) // bucket contains workers for id
              .setMemtablePrefixBloomSizeRatio(0.02) // for fast bucket memtable lookups
              .optimizeLevelStyleCompaction() // use level style compaction
              .setMemtableWholeKeyFiltering(true) // fast point lookups for bucket id
              .setCompressionType(CompressionType.NO_COMPRESSION)));
    }

    mToClose.addAll(columns.stream().map(
        ColumnFamilyDescriptor::getOptions).collect(Collectors.toList()));

    Filter filterPolicy = new BloomFilter();
    mToClose.add(filterPolicy);
    // Set the block meta column options
    Cache inodeCache = new LRUCache(ServerConfiguration.getInt(
        PropertyKey.MATER_METASTORE_ROCKS_BLOCK_META_CACHE_SIZE));
    mToClose.add(inodeCache);
    columns.get(0).getOptions()
        // the table format configuration is set here as it is not able to be set from the
        // configuration file
        .setTableFormatConfig(new BlockBasedTableConfig()
            .setIndexType(ServerConfiguration.getEnum(
                PropertyKey.MASTER_METASTORE_ROCKS_BLOCK_META_INDEX, IndexType.class))
            .setDataBlockIndexType(ServerConfiguration.getEnum(
                PropertyKey.MASTER_METASTORE_ROCKS_BLOCK_META_BLOCK_INDEX,
                DataBlockIndexType.class))
            .setBlockCache(inodeCache)
            .setFilterPolicy(filterPolicy));
    // Set the block location column options
    Cache edgeCache = new LRUCache(ServerConfiguration.getInt(
        PropertyKey.MATER_METASTORE_ROCKS_BLOCK_LOCATION_CACHE_SIZE));
    mToClose.add(edgeCache);
    columns.get(1).getOptions()
        // the table format configuration is set here as it is not able to be set from the
        // configuration file
        .setTableFormatConfig(new BlockBasedTableConfig()
            .setIndexType(ServerConfiguration.getEnum(
                PropertyKey.MASTER_METASTORE_ROCKS_BLOCK_LOCATION_INDEX, IndexType.class))
            .setDataBlockIndexType(ServerConfiguration.getEnum(
                PropertyKey.MASTER_METASTORE_ROCKS_BLOCK_LOCATION_BLOCK_INDEX,
                DataBlockIndexType.class))
            .setBlockCache(edgeCache)
            .setFilterPolicy(filterPolicy));
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
        Arrays.asList(mBlockMetaColumn, mBlockLocationsColumn));
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
    mReadPrefixSameAsStart.close();
    mToClose.forEach(RocksObject::close);
    LOG.info("RocksBlockStore closed");
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
    try (final RocksIterator iter = db().newIterator(mBlockLocationsColumn.get(),
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
