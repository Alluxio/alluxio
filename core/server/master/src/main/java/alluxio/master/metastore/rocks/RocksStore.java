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

import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointOutputStream;
import alluxio.master.journal.checkpoint.CheckpointType;
import alluxio.util.TarUtils;
import alluxio.util.io.FileUtils;

import com.google.common.base.Preconditions;
import org.rocksdb.BackupEngine;
import org.rocksdb.BackupableDBOptions;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.RestoreOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for managing a rocksdb database. This class handles common functionality such as
 * initializing the database and performing database backup/restore.
 */
public final class RocksStore implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(RocksStore.class);

  private final String mDbPath;
  private final String mDbBackupPath;
  private final Collection<ColumnFamilyDescriptor> mColumnFamilyDescriptors;
  private final DBOptions mDbOpts;

  private RocksDB mDb;
  /** Map from column name to column handle. */
  private Map<String, ColumnFamilyHandle> mColumnFamilyHandles;

  /**
   * @param dbPath a path for the rocks database
   * @param backupPath a path for taking database backups
   * @param columnFamilyDescriptors columns to create within the rocks database
   * @param dbOpts db options
   */
  public RocksStore(String dbPath, String backupPath,
      Collection<ColumnFamilyDescriptor> columnFamilyDescriptors, DBOptions dbOpts) {
    mDbPath = dbPath;
    mDbBackupPath = backupPath;
    mColumnFamilyDescriptors = columnFamilyDescriptors;
    mDbOpts = dbOpts;
    new File(mDbBackupPath).mkdirs();
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
   * @param columnName a column name
   * @return the column family handle for the given name. The columns change when clear() is called,
   *         so if the caller caches the returned column, they must reset it after calling clear()
   */
  public synchronized ColumnFamilyHandle getColumn(String columnName) {
    return mColumnFamilyHandles.get(columnName);
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
  }

  private synchronized void resetDb() throws RocksDBException {
    stopDb();
    formatDbDirs();
    createDb();
  }

  private synchronized void stopDb() {
    if (mDb != null) {
      try {
        // Column handles must be closed before closing the db, or an exception gets thrown.
        mColumnFamilyHandles.values().forEach(handle -> handle.close());
        mDb.close();
      } catch (Throwable t) {
        LOG.error("Failed to close rocks database", t);
      }
      mColumnFamilyHandles = null;
      mDb = null;
    }
  }

  private void formatDbDirs() {
    try {
      if (FileUtils.exists(mDbPath)) {
        FileUtils.deletePathRecursively(mDbPath);
      }
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
    mDb = RocksDB.open(mDbOpts, mDbPath, cfDescriptors, columns);
    mColumnFamilyHandles = new HashMap<>();
    for (ColumnFamilyHandle column : columns) {
      mColumnFamilyHandles.put(new String(column.getName()), column);
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
    LOG.info("Writing rocksdb checkpoint");
    long startNano = System.nanoTime();
    CheckpointOutputStream out = new CheckpointOutputStream(output, CheckpointType.ROCKS);
    try (BackupEngine backupEngine = BackupEngine.open(Env.getDefault(),
        new BackupableDBOptions(mDbBackupPath).setMaxBackgroundOperations(4))) {
      backupEngine.createNewBackup(mDb, true);
      backupEngine.purgeOldBackups(1);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
    LOG.info("Backed up rocks database, creating tarball");
    TarUtils.writeTarGz(Paths.get(mDbBackupPath), out);
    LOG.info("Completed rocksdb checkpoint in {}ms", (System.nanoTime() - startNano) / 1_000_000);
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
    FileUtils.deletePathRecursively(mDbBackupPath);
    TarUtils.readTarGz(Paths.get(mDbBackupPath), input);
    try (BackupEngine backupEngine = BackupEngine.open(Env.getDefault(),
        new BackupableDBOptions(mDbBackupPath).setMaxBackgroundOperations(4))) {
      backupEngine.restoreDbFromLatestBackup(mDbPath, mDbPath, new RestoreOptions(false));
    } catch (RocksDBException e) {
      throw new IOException(String.format("Failed to restore %s from backup %s: %s", mDbPath,
          mDbBackupPath, e.toString()), e);
    }
    try {
      createDb();
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
    LOG.info("Restored rocksdb checkpoint in {}ms", (System.nanoTime() - startNano) / 1_000_000);
  }

  @Override
  public synchronized void close() {
    stopDb();
  }
}
