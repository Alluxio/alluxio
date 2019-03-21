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
import alluxio.master.metastore.BlockStore;
import alluxio.proto.meta.Block.BlockLocation;
import alluxio.proto.meta.Block.BlockMeta;
import alluxio.util.io.PathUtils;

import com.google.common.primitives.Longs;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.HashLinkedListMemTableConfig;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

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

  // This is a field instead of a constant because it depends on the call to RocksDB.loadLibrary().
  private final WriteOptions mDisableWAL;

  private final RocksStore mRocksStore;

  private RocksDB mDb;
  private ColumnFamilyHandle mBlockMetaColumn;
  private ColumnFamilyHandle mBlockLocationsColumn;

  /**
   * Creates and initializes a rocks block store.
   *
   * @param args block store args
   */
  public RocksBlockStore(BlockStoreArgs args) {
    String baseDir = args.getConfiguration().get(PropertyKey.MASTER_METASTORE_DIR);
    RocksDB.loadLibrary();
    mDisableWAL = new WriteOptions().setDisableWAL(true);
    ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
        .setMemTableConfig(new HashLinkedListMemTableConfig())
        .setCompressionType(CompressionType.NO_COMPRESSION)
        .useFixedLengthPrefixExtractor(8); // We always search using the initial long key
    List<ColumnFamilyDescriptor> columns =
        Arrays.asList(new ColumnFamilyDescriptor(BLOCK_META_COLUMN.getBytes(), cfOpts),
            new ColumnFamilyDescriptor(BLOCK_LOCATIONS_COLUMN.getBytes(), cfOpts));
    DBOptions dbOpts = new DBOptions()
        // Concurrent memtable write is not supported for hash linked list memtable
        .setAllowConcurrentMemtableWrite(false)
        .setMaxOpenFiles(-1)
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
    String dbPath = PathUtils.concatPath(baseDir, BLOCKS_DB_NAME);
    String backupPath = PathUtils.concatPath(baseDir, BLOCKS_DB_NAME + "-backups");
    mRocksStore = new RocksStore(dbPath, backupPath, columns, dbOpts);
    setDbAndColumns(mRocksStore);
  }

  @Override
  public Optional<BlockMeta> getBlock(long id) {
    byte[] meta;
    try {
      meta = mDb.get(mBlockMetaColumn, Longs.toByteArray(id));
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
      // Overwrites the key if it already exists.
      mDb.put(mBlockMetaColumn, mDisableWAL, Longs.toByteArray(id), meta.toByteArray());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeBlock(long id) {
    try {
      mDb.delete(mBlockMetaColumn, mDisableWAL, Longs.toByteArray(id));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() {
    mRocksStore.clear();
    setDbAndColumns(mRocksStore);
  }

  @Override
  public List<BlockLocation> getLocations(long id) {
    try (RocksIterator iter =
        mDb.newIterator(mBlockLocationsColumn, new ReadOptions().setPrefixSameAsStart(true))) {
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
      mDb.put(mBlockLocationsColumn, mDisableWAL, key, location.toByteArray());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeLocation(long blockId, long workerId) {
    byte[] key = RocksUtils.toByteArray(blockId, workerId);
    try {
      mDb.delete(mBlockLocationsColumn, mDisableWAL, key);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterator<Block> iterator() {
    List<Block> blocks = new ArrayList<>();
    try (RocksIterator iter =
        mDb.newIterator(mBlockMetaColumn, new ReadOptions().setPrefixSameAsStart(true))) {
      iter.seekToFirst();
      while (iter.isValid()) {
        try {
          blocks.add(new Block(Longs.fromByteArray(iter.key()), BlockMeta.parseFrom(iter.value())));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        iter.next();
      }
    }
    return blocks.iterator();
  }

  /**
   * @param store the RocksStore to set the db and columns from
   */
  private void setDbAndColumns(RocksStore store) {
    mDb = store.getDb();
    if (mBlockLocationsColumn != null) {
      mBlockLocationsColumn.close();
    }
    mBlockLocationsColumn = store.getColumn(BLOCK_LOCATIONS_COLUMN);
    if (mBlockMetaColumn != null) {
      mBlockMetaColumn.close();
    }
    mBlockMetaColumn = store.getColumn(BLOCK_META_COLUMN);
  }
}
