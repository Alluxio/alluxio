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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.master.metastore.BlockStore;
import alluxio.proto.meta.Block.BlockLocation;
import alluxio.proto.meta.Block.BlockMeta;
import alluxio.util.io.FileUtils;

import com.google.common.primitives.Longs;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Block store backed by RocksDB.
 */
public class RocksBlockStore implements BlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(RocksBlockStore.class);
  private static final String BLOCKS_DB_NAME = "blocks";
  private static final String BLOCK_META_COLUMN = "block-meta";
  private static final String BLOCK_LOCATIONS_COLUMN = "block-locations";

  private final String mBaseDir;

  private String mDbPath;
  private RocksDB mDb;
  private ColumnFamilyHandle mDefaultColumn;
  private ColumnFamilyHandle mBlockMetaColumn;
  private ColumnFamilyHandle mBlockLocationsColumn;

  /**
   * Creates and initializes a rocks block store.
   */
  public RocksBlockStore() throws RocksDBException {
    mBaseDir = Configuration.get(PropertyKey.MASTER_METASTORE_DIR);
    RocksDB.loadLibrary();
    initDb();
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
      mDb.put(mBlockMetaColumn, Longs.toByteArray(id), meta.toByteArray());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeBlock(long id) {
    try {
      mDb.delete(mBlockMetaColumn, Longs.toByteArray(id));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() {
    try {
      initDb();
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<BlockLocation> getLocations(long id) {
    RocksIterator iter =
        mDb.newIterator(mBlockLocationsColumn, new ReadOptions().setPrefixSameAsStart(true));
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

  @Override
  public void addLocation(long id, BlockLocation location) {
    byte[] key = RocksUtils.toByteArray(id, location.getWorkerId());
    try {
      mDb.put(mBlockLocationsColumn, key, location.toByteArray());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeLocation(long blockId, long workerId) {
    byte[] key = RocksUtils.toByteArray(blockId, workerId);
    try {
      mDb.delete(mBlockLocationsColumn, key);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterator<Block> iterator() {
    RocksIterator iter =
        mDb.newIterator(mBlockMetaColumn, new ReadOptions().setPrefixSameAsStart(true));
    iter.seekToFirst();
    return new Iterator<Block>() {
      @Override
      public boolean hasNext() {
        return iter.isValid();
      }

      @Override
      public Block next() {
        try {
          return new Block(Longs.fromByteArray(iter.key()), BlockMeta.parseFrom(iter.value()));
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          iter.next();
        }
      }
    };
  }

  private void initDb() throws RocksDBException {
    if (mDb != null) {
      try {
        // Column handles must be closed before closing the db, or an exception gets thrown.
        mDefaultColumn.close();
        mBlockMetaColumn.close();
        mBlockLocationsColumn.close();
        mDb.close();
      } catch (Throwable t) {
        LOG.error("Failed to close previous rocks database", t);
      }
      try {
        FileUtils.deletePathRecursively(mDbPath);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    new File(mBaseDir).mkdirs();

    ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
        .useFixedLengthPrefixExtractor(8); // We always search using the initial long key

    List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
        new ColumnFamilyDescriptor(BLOCK_META_COLUMN.getBytes(), cfOpts),
        new ColumnFamilyDescriptor(BLOCK_LOCATIONS_COLUMN.getBytes(), cfOpts)
    );

    DBOptions options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);

    // a list which will hold the handles for the column families once the db is opened
    List<ColumnFamilyHandle> columns = new ArrayList<>();
    mDbPath = RocksUtils.generateDbPath(mBaseDir, BLOCKS_DB_NAME);
    mDb = RocksDB.open(options, mDbPath, cfDescriptors, columns);
    mDefaultColumn = columns.get(0);
    mBlockMetaColumn = columns.get(1);
    mBlockLocationsColumn = columns.get(2);

    LOG.info("Created new rocks database under path {}", mDbPath);
  }
}
