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

import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointOutputStream;
import alluxio.master.journal.checkpoint.CheckpointType;
import alluxio.retry.TimeoutRetry;
import alluxio.util.ParallelZipUtils;
import alluxio.util.TarUtils;
import alluxio.util.io.FileUtils;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.DataBlockIndexType;
import org.rocksdb.Filter;
import org.rocksdb.IndexType;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for managing a rocksdb database. This class handles common functionality such as
 * initializing the database and performing database backup/restore.
 *
 * Thread safety is achieved by synchronizing all public methods.
 */
@ThreadSafe
public final class RocksStore implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(RocksStore.class);
  public static final int ROCKS_OPEN_RETRY_TIMEOUT = 20 * Constants.SECOND_MS;
  private final String mName;
  private final String mDbPath;
  private final String mDbCheckpointPath;
  private final String mParallelBackupPath;
  private final Integer mParallelBackupPoolSize;
  private final Collection<ColumnFamilyDescriptor> mColumnFamilyDescriptors;
  private final DBOptions mDbOpts;

  private RocksDB mDb;
  private Checkpoint mCheckpoint;
  // When we create the database, we must set these handles.
  private final List<AtomicReference<ColumnFamilyHandle>> mColumnHandles;

  /**
   * @param name a name to distinguish what store this is
   * @param dbPath a path for the rocks database
   * @param checkpointPath a path for taking database checkpoints
   * @param dbOpts the configured RocksDB options
   * @param columnFamilyDescriptors columns to create within the rocks database
   * @param columnHandles column handle references to populate
   */
  public RocksStore(String name, String dbPath, String checkpointPath,
      DBOptions dbOpts,
      Collection<ColumnFamilyDescriptor> columnFamilyDescriptors,
      List<AtomicReference<ColumnFamilyHandle>> columnHandles) {
    this(name, dbPath, checkpointPath, null, dbOpts, columnFamilyDescriptors, columnHandles);
  }

  /**
   * @param name a name to distinguish what store this is
   * @param dbPath a path for the rocks database
   * @param checkpointPath a path for taking database checkpoints
   * @param parallelBackupPath a path for taking database backup in parallel
   * @param dbOpts the configured RocksDB options
   * @param columnFamilyDescriptors columns to create within the rocks database
   * @param columnHandles column handle references to populate
   */
  public RocksStore(String name, String dbPath, String checkpointPath, String parallelBackupPath,
      DBOptions dbOpts,
      Collection<ColumnFamilyDescriptor> columnFamilyDescriptors,
      List<AtomicReference<ColumnFamilyHandle>> columnHandles) {
    Preconditions.checkState(columnFamilyDescriptors.size() == columnHandles.size());
    mName = name;
    mDbPath = dbPath;
    mDbCheckpointPath = checkpointPath;
    mParallelBackupPath = parallelBackupPath;
    mParallelBackupPoolSize = Configuration.getInt(
        PropertyKey.MASTER_METASTORE_ROCKS_PARALLEL_BACKUP_THREADS);
    mColumnFamilyDescriptors = columnFamilyDescriptors;
    mDbOpts = dbOpts;
    mColumnHandles = columnHandles;
    try {
      resetDb();
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return the underlying rocksdb instance. The instance changes when clear() is called, so if the
   *         caller caches the returned db, they must reset it after calling clear()
   */
  public synchronized RocksDB getDb() {
    return mDb;
  }

  /**
   * Clears and re-initializes the database.
   */
  public synchronized void clear() {
    try {
      resetDb();
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    LOG.info("Cleared store at {}", mDbPath);
  }

  private void resetDb() throws RocksDBException {
    stopDb();
    formatDbDirs();
    createDb();
  }

  private void stopDb() {
    LOG.info("Closing {} rocks database", mName);
    if (mDb != null) {
      try {
        // Column handles must be closed before closing the db, or an exception gets thrown.
        mColumnHandles.forEach(handle -> {
          if (handle != null) {
            handle.get().close();
            handle.set(null);
          }
        });
        mDb.close();
        mCheckpoint.close();
      } catch (Throwable t) {
        LOG.error("Failed to close rocks database", t);
      }
      mDb = null;
      mCheckpoint = null;
    }
  }

  private void formatDbDirs() {
    try {
      FileUtils.deletePathRecursively(mDbPath);
      FileUtils.deletePathRecursively(mDbCheckpointPath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    new File(mDbPath).mkdirs();
  }

  private void createDb() throws RocksDBException {
    List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
    cfDescriptors.addAll(mColumnFamilyDescriptors);
    // a list which will hold the handles for the column families once the db is opened
    List<ColumnFamilyHandle> columns = new ArrayList<>();
    final TimeoutRetry retryPolicy = new TimeoutRetry(ROCKS_OPEN_RETRY_TIMEOUT, 100);
    RocksDBException lastException = null;
    while (retryPolicy.attempt()) {
      try {
        mDb = RocksDB.open(mDbOpts, mDbPath, cfDescriptors, columns);
        break;
      } catch (RocksDBException e) {
        // sometimes the previous terminated process's lock may not have been fully cleared yet
        // retry until timeout to make sure that isn't the case
        lastException = e;
      }
    }
    if (mDb == null && lastException != null) {
      throw lastException;
    }
    mCheckpoint = Checkpoint.create(mDb);
    for (int i = 0; i < columns.size() - 1; i++) {
      // Skip the default column.
      mColumnHandles.get(i).set(columns.get(i + 1));
    }
    LOG.info("Opened rocks database under path {}", mDbPath);
  }

  /**
   * Writes a checkpoint of the database's content to the given output stream.
   *
   * @param output the stream to write to
   */
  public synchronized void writeToCheckpoint(OutputStream output)
      throws IOException, InterruptedException {
    LOG.info("Creating rocksdb checkpoint at {}", mDbCheckpointPath);
    long startNano = System.nanoTime();

    CheckpointOutputStream out = new CheckpointOutputStream(output, CheckpointType.ROCKS);
    try {
      // createCheckpoint requires that the directory not already exist.
      FileUtils.deletePathRecursively(mDbCheckpointPath);
      mCheckpoint.createCheckpoint(mDbCheckpointPath);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
    LOG.info("Checkpoint complete, creating tarball");
    if (mParallelBackupPath == null) {
      TarUtils.writeTarGz(Paths.get(mDbCheckpointPath), out);
    } else {
      ParallelZipUtils.compress(Paths.get(mDbCheckpointPath), out,
          mParallelBackupPoolSize);
    }

    LOG.info("Completed rocksdb checkpoint in {}ms", (System.nanoTime() - startNano) / 1_000_000);
    // Checkpoint is no longer needed, delete to save space.
    FileUtils.deletePathRecursively(mDbCheckpointPath);
  }

  /**
   * Restores the database from a checkpoint.
   *
   * @param input the checkpoint stream to restore from
   */
  public synchronized void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    LOG.info("Restoring rocksdb from checkpoint");
    long startNano = System.nanoTime();
    Preconditions.checkState(input.getType() == CheckpointType.ROCKS,
        "Unexpected checkpoint type in RocksStore: " + input.getType());
    stopDb();
    FileUtils.deletePathRecursively(mDbPath);

    if (mParallelBackupPath == null) {
      TarUtils.readTarGz(Paths.get(mDbPath), input);
    } else {
      FileUtils.deletePathRecursively(mParallelBackupPath);
      try (FileOutputStream fos = new FileOutputStream(mParallelBackupPath)) {
        IOUtils.copy(input, fos);
      }
      ParallelZipUtils.decompress(Paths.get(mDbPath), mParallelBackupPath,
          mParallelBackupPoolSize);
      FileUtils.deletePathRecursively(mParallelBackupPath);
    }
    try {
      createDb();
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
    LOG.info("Restored rocksdb checkpoint in {}ms",
        (System.nanoTime() - startNano) / Constants.MS_NANO);
  }

  @Override
  public synchronized void close() {
    stopDb();
    LOG.info("Closed store at {}", mDbPath);
  }

  // helper function to load RockDB configuration options based on property key configurations.
  static Optional<BlockBasedTableConfig> checkSetTableConfig(
      PropertyKey cacheSize, PropertyKey bloomFilter, PropertyKey indexType,
      PropertyKey blockIndexType, List<RocksObject> toClose) {
    // The following options are set by property keys as they are not able to be
    // set using configuration files.
    BlockBasedTableConfig blockConfig = new BlockBasedTableConfig();
    boolean shoudSetConfig = false;
    if (Configuration.isSet(cacheSize)) {
      shoudSetConfig = true;
      // Set the inodes column options
      Cache inodeCache = new LRUCache(Configuration.getInt(cacheSize));
      toClose.add(inodeCache);
      blockConfig.setBlockCache(inodeCache);
    }
    if (Configuration.getBoolean(bloomFilter)) {
      shoudSetConfig = true;
      Filter filter = new BloomFilter();
      toClose.add(filter);
      blockConfig.setFilterPolicy(filter);
    }
    if (Configuration.isSet(indexType)) {
      shoudSetConfig = true;
      blockConfig.setIndexType(toRocksIndexType(Configuration.getEnum(
          indexType, alluxio.master.metastore.rocks.IndexType.class)));
    }
    if (Configuration.isSet(blockIndexType)) {
      shoudSetConfig = true;
      blockConfig.setDataBlockIndexType(toRocksDataBlockIndexType(Configuration.getEnum(
          blockIndexType, alluxio.master.metastore.rocks.DataBlockIndexType.class)));
    }
    if (shoudSetConfig) {
      return Optional.of(blockConfig);
    }
    return Optional.empty();
  }

  // helper function to convert alluxio enum to rocksDb enum
  private static DataBlockIndexType toRocksDataBlockIndexType(
      alluxio.master.metastore.rocks.DataBlockIndexType index) {
    switch (index) {
      case kDataBlockBinarySearch:
        return DataBlockIndexType.kDataBlockBinarySearch;
      case kDataBlockBinaryAndHash:
        return DataBlockIndexType.kDataBlockBinaryAndHash;
      default:
        throw new IllegalArgumentException(String.format("Unknown DataBlockIndexType %s", index));
    }
  }

  // helper function to convert alluxio enum to rocksDb enum
  private static IndexType toRocksIndexType(
      alluxio.master.metastore.rocks.IndexType index) {
    switch (index) {
      case kBinarySearch:
        return IndexType.kBinarySearch;
      case kHashSearch:
        return IndexType.kHashSearch;
      case kBinarySearchWithFirstKey:
        return IndexType.kBinarySearchWithFirstKey;
      case kTwoLevelIndexSearch:
        return IndexType.kTwoLevelIndexSearch;
      default:
        throw new IllegalArgumentException(String.format("Unknown IndexType %s", index));
    }
  }
}
