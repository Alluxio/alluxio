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
import alluxio.exception.ExceptionMessage;
import alluxio.exception.runtime.UnavailableRuntimeException;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointOutputStream;
import alluxio.master.journal.checkpoint.CheckpointType;
import alluxio.retry.CountingRetry;
import alluxio.retry.TimeoutRetry;
import alluxio.util.compression.ParallelZipUtils;
import alluxio.util.compression.TarUtils;
import alluxio.util.SleepUtils;
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
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class for managing a rocksdb database. This class handles common functionality such as
 * initializing the database and performing database backup/restore.
 *
 * This class provides locking methods for the callers. And the thread safety of RocksDB
 * relies on the caller to use the corresponding lock methods.
 * The reasons why this class only provides thread safety utilities to the callers
 * (instead of wrapping it under each call) are:
 * 1. Callers like RocksInodeStore and RocksBlockMetaStore have specific read/write logic
 *    like iteration, which cannot be abstracted and locked internally in this class.
 * 2. With locking methods provided by this class, callers like RocksInodeStore
 *    can actually reuse the locks to perform concurrency control on their own logic.
 *
 * For reading/writing on the RocksDB, use the shared lock
 * <blockquote><pre>
 *   try (RocksSharedLockHandle r = mRocksStore.checkAndAcquireSharedLock() {
 *     // perform your read/write operation
 *   }
 * </pre></blockquote>
 *
 * For operations like closing/restart/restoring on the RocksDB, an exclusive lock should
 * be acquired by calling one of:
 * 1. {@link #lockForClosing()}
 * 2. {@link #lockForRestart()}
 */
@NotThreadSafe
public final class RocksStore implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(RocksStore.class);
  public static final int ROCKS_OPEN_RETRY_TIMEOUT = 20 * Constants.SECOND_MS;
  public static final Duration ROCKS_CLOSE_WAIT_TIMEOUT =
      Configuration.getDuration(PropertyKey.MASTER_METASTORE_ROCKS_EXCLUSIVE_LOCK_TIMEOUT);

  private final String mName;
  private final String mDbPath;
  private final String mDbCheckpointPath;
  private final Integer mParallelBackupPoolSize;

  private final int mCompressLevel = Configuration.getInt(
      PropertyKey.MASTER_EMBEDDED_JOURNAL_SNAPSHOT_REPLICATION_COMPRESSION_LEVEL);
  private final boolean mParallelBackup = Configuration.getBoolean(
      PropertyKey.MASTER_METASTORE_ROCKS_PARALLEL_BACKUP);

  /*
   * Below 2 fields are created and managed by the external user class,
   * no need to close in this class.
   */
  private final Collection<ColumnFamilyDescriptor> mColumnFamilyDescriptors;
  private final DBOptions mDbOpts;
  /*
   * Below 3 fields are created and managed internally to this class,
   * must be closed in this class.
   */
  private volatile RocksDB mDb;
  private volatile Checkpoint mCheckpoint;
  private final List<AtomicReference<ColumnFamilyHandle>> mColumnHandles;

  public final AtomicBoolean mStopServing = new AtomicBoolean(false);
  public final LongAdder mRefCount = new LongAdder();

  /**
   * @param name a name to distinguish what store this is
   * @param dbPath a path for the rocks database
   * @param checkpointPath a path for taking database checkpoints
   * @param dbOpts the configured RocksDB options
   * @param columnFamilyDescriptors columns to create within the rocks database
   * @param columnHandles column handle references to populate
   */
  public RocksStore(String name, String dbPath, String checkpointPath, DBOptions dbOpts,
      Collection<ColumnFamilyDescriptor> columnFamilyDescriptors,
      List<AtomicReference<ColumnFamilyHandle>> columnHandles) {
    Preconditions.checkState(columnFamilyDescriptors.size() == columnHandles.size());
    mName = name;
    mDbPath = dbPath;
    mDbCheckpointPath = checkpointPath;
    mParallelBackupPoolSize = Configuration.getInt(
        PropertyKey.MASTER_METASTORE_ROCKS_PARALLEL_BACKUP_THREADS);
    mColumnFamilyDescriptors = columnFamilyDescriptors;
    mDbOpts = dbOpts;
    mColumnHandles = columnHandles;
    LOG.info("Resetting RocksDB for {} on init", name);
    try (RocksExclusiveLockHandle lock = lockForRestart()) {
      resetDb();
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Requires the caller to acquire a shared lock by calling {@link #checkAndAcquireSharedLock()}.
   *
   * @return the underlying rocksdb instance. The instance changes when clear() is called, so if the
   *         caller caches the returned db, they must reset it after calling clear()
   */
  public RocksDB getDb() {
    return mDb;
  }

  /**
   * Clears and re-initializes the database.
   * Requires the caller to acquire exclusive lock by calling {@link #lockForRestart()}.
   */
  public void clear() {
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
   * Writes a checkpoint under the specified directory.
   * @param directory that the checkpoint will be written under
   * @throws RocksDBException if it encounters and error when writing the checkpoint
   */
  public synchronized void writeToCheckpoint(File directory) throws RocksDBException {
    mCheckpoint.createCheckpoint(directory.getPath());
  }

  /**
   * Writes a checkpoint of the database's content to the given output stream.
   * Requires the caller to acquire an exclusive lock by calling {@link #lockForRestart()}.
   *
   * @param output the stream to write to
   */
  public void writeToCheckpoint(OutputStream output)
      throws IOException, InterruptedException {
    LOG.info("Creating rocksdb checkpoint at {}", mDbCheckpointPath);
    long startNano = System.nanoTime();

    try {
      // createCheckpoint requires that the directory not already exist.
      FileUtils.deletePathRecursively(mDbCheckpointPath);
      mCheckpoint.createCheckpoint(mDbCheckpointPath);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }

    if (mParallelBackup) {
      CheckpointOutputStream out = new CheckpointOutputStream(output,
          CheckpointType.ROCKS_PARALLEL);
      LOG.info("Checkpoint complete, compressing with {} threads", mParallelBackupPoolSize);
      ParallelZipUtils.compress(Paths.get(mDbCheckpointPath), out,
          mParallelBackupPoolSize, mCompressLevel);
    } else {
      CheckpointOutputStream out = new CheckpointOutputStream(output, CheckpointType.ROCKS_SINGLE);
      LOG.info("Checkpoint complete, compressing with one thread");
      TarUtils.writeTarGz(Paths.get(mDbCheckpointPath), out, mCompressLevel);
    }

    LOG.info("Completed rocksdb checkpoint in {}ms", (System.nanoTime() - startNano) / 1_000_000);
    // Checkpoint is no longer needed, delete to save space.
    FileUtils.deletePathRecursively(mDbCheckpointPath);
  }

  /**
   * Restores RocksDB state from a checkpoint at the provided location. Moves the directory to a
   * permanent location, restores RocksDB state, and then immediately takes a new snapshot in the
   * original location as replacement.
   * @param directory where the checkpoint is located
   * @throws RocksDBException if rocks encounters a problem
   * @throws IOException if moving files around encounters a problem
   */
  public synchronized void restoreFromCheckpoint(File directory)
      throws RocksDBException, IOException {
    stopDb();
    File dbPath = new File(mDbPath);
    org.apache.commons.io.FileUtils.deleteDirectory(dbPath);
    org.apache.commons.io.FileUtils.moveDirectory(directory, dbPath);
    createDb();
    writeToCheckpoint(directory);
  }

  /**
   * Restores the database from a checkpoint.
   * Requires the caller to acquire an exclusive lock by calling {@link #lockForRestart()}.
   *
   * @param input the checkpoint stream to restore from
   */
  public void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    LOG.info("Restoring rocksdb from checkpoint");
    long startNano = System.nanoTime();
    Preconditions.checkState(input.getType() == CheckpointType.ROCKS_SINGLE
        || input.getType() == CheckpointType.ROCKS_PARALLEL,
        "Unexpected checkpoint type in RocksStore: " + input.getType());
    stopDb();
    FileUtils.deletePathRecursively(mDbPath);

    if (input.getType() == CheckpointType.ROCKS_PARALLEL) {
      List<String> tmpDirs = Configuration.getList(PropertyKey.TMP_DIRS);
      String tmpZipFilePath = new File(tmpDirs.get(0), "alluxioRockStore-" + UUID.randomUUID())
          .getPath();

      try {
        try (FileOutputStream fos = new FileOutputStream(tmpZipFilePath)) {
          IOUtils.copy(input, fos);
        }

        ParallelZipUtils.decompress(Paths.get(mDbPath), tmpZipFilePath,
            mParallelBackupPoolSize);

        FileUtils.deletePathRecursively(tmpZipFilePath);
      } catch (Exception e) {
        LOG.warn("Failed to decompress checkpoint from {} to {}", tmpZipFilePath, mDbPath);
        throw e;
      }
    } else {
      TarUtils.readTarGz(Paths.get(mDbPath), input);
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
  /**
   * Requires the caller to acquire exclusive lock by calling {@link #lockForClosing()}.
   */
  public void close() {
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

  /**
   * Before any r/w operation on the RocksDB, acquire a shared lock with this method.
   * The shared lock guarantees the RocksDB will not be restarted/cleared during the
   * r/w access.
   */
  public RocksSharedLockHandle checkAndAcquireSharedLock() {
    if (mStopServing.get()) {
      throw new UnavailableRuntimeException(ExceptionMessage.ROCKS_DB_CLOSING.getMessage());
    }
    /*
     * The lock action is merely incrementing the lock so it is very fast
     * The closer will respect the ref count and only close when the ref count is zero
     */
    mRefCount.increment();

    /*
     * Need to check the flag again to PREVENT the sequence of events below:
     * 1. Reader checks flag
     * 2. Closer sets flag
     * 3. Closer sees refCount=0
     * 4. Reader increments refCount
     * 5. Closer closes RocksDB
     * 6. Reader reads RocksDB and incurs a segfault
     *
     * With the 2nd check, we make sure the ref count will be respected by the closer and
     * the closer will therefore wait for this reader to complete/abort.
     */
    if (mStopServing.get()) {
      mRefCount.decrement();
      throw new UnavailableRuntimeException(ExceptionMessage.ROCKS_DB_CLOSING.getMessage());
    }
    return new RocksSharedLockHandle(mRefCount);
  }

  private void blockingWait() {
    mStopServing.set(true);

    /*
    * Wait until:
    * 1. Ref count is zero, meaning all concurrent r/w have completed or aborted
    * 2. Timeout is reached, meaning we force close/restart without waiting
    *
    * According to Java doc
    * https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/atomic/LongAdder.html
    * In absence of concurrent updates, sum() returns an accurate result.
    * But sum() does not see concurrent updates and therefore can miss an update.
    *
    * The correctness then relies on the 2nd check in checkAndAcquireSharedLock()
    * because the reader will see the flag and just abort voluntarily. An example sequence
    * of events is like below:
    * 1. Reader checks flag, the flag is not set by the closer
    * 2. Closer sets flag
    * 3. Closer sees refCount=0
    * 4. Reader increments refCount
    * 5. Closer closes RocksDB
    * 6. Reader checks flag again and sees the flag
    * 7. Reader decrements refCount aborts in checkAndAcquireSharedLock()
    */
    Instant waitStart = Instant.now();
    CountingRetry retry = new CountingRetry((int) ROCKS_CLOSE_WAIT_TIMEOUT.getSeconds() * 10);
    while (mRefCount.sum() != 0 && retry.attempt()) {
      SleepUtils.sleepMs(100);
    }
    Duration elapsed = Duration.between(waitStart, Instant.now());
    LOG.info("Waited {}ms for ongoing read/write to complete/abort", elapsed.toMillis());
    System.out.println("Waited " + elapsed.toMillis() + " for ongoing read/write to complete/abort");

    /*
     * Reset the ref count to forget about the aborted operations
     */
    long unclosedOperations = mRefCount.sumThenReset();
    if (unclosedOperations != 0) {
      if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
        ThreadInfo[] allThreads = ManagementFactory.getThreadMXBean().dumpAllThreads(true, true);
        throw new RuntimeException("ref count=" + unclosedOperations + " some operations are not updating ref count correctly!");
      }
      LOG.warn("{} readers/writers fail to complete/abort before we stop/restart the RocksDB",
          unclosedOperations);
    }
  }

  /**
   * Before the process shuts down, acquire an exclusive lock on the RocksDB before closing.
   * Note this lock only exists on the Alluxio side. A STOP_SERVING flag will be set so all
   * existing readers/writers will abort asap.
   * The exclusive lock ensures there are no existing concurrent r/w operations, so it is safe to
   * close the RocksDB and recycle all relevant resources.
   *
   * The STOP_SERVING status will NOT be reset, because the process will shut down soon.
   */
  public RocksExclusiveLockHandle lockForClosing() {
    blockingWait();
    return new RocksExclusiveLockHandle(false, mStopServing, mRefCount);
  }

  /**
   * Before the process shuts down, acquire an exclusive lock on the RocksDB before closing.
   * Note this lock only exists on the Alluxio side. A STOP_SERVING flag will be set so all
   * existing readers/writers will abort asap.
   * The exclusive lock ensures there are no existing concurrent r/w operations, so it is safe to
   * restart/checkpoint the RocksDB and update the DB reference.
   *
   * The STOP_SERVING status will be reset and the RocksDB will be open for operations again.
   * See {@link #checkAndAcquireSharedLock} for how this affects the shared lock logic.
   */
  public RocksExclusiveLockHandle lockForRestart() {
    blockingWait();
    return new RocksExclusiveLockHandle(true, mStopServing, mRefCount);
  }

  /**
   * Used by ongoing r/w operations to check if the operation needs to abort and yield
   * to the RocksDB shutdown.
   */
  public void abortIfClosing() {
    if (mStopServing.get()) {
      throw new UnavailableRuntimeException(ExceptionMessage.ROCKS_DB_CLOSING.getMessage());
    }
  }

  /**
   * Checks whether the RocksDB is marked for exclusive access, so the operation should abort.
   */
  public boolean isServiceStopping() {
    return mStopServing.get();
  }
}
