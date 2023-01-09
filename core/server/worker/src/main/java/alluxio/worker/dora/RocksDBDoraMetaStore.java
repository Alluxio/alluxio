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

package alluxio.worker.dora;

import alluxio.proto.meta.DoraMeta;
import alluxio.rocks.RocksStore;
import alluxio.util.io.PathUtils;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.HashLinkedListMemTableConfig;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksObject;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Dora Meta Store in RocksDB.
 */
public class RocksDBDoraMetaStore implements DoraMetaStore {
  private static final Logger LOG = LoggerFactory.getLogger(RocksDBDoraMetaStore.class);
  private static final String DORA_META_DB_NAME = "DoraMeta";
  private static final String DORA_META_FILE_STATUS_COLUMN = "FileStatusCF";
  private static final String DORA_META_STORE_NAME = "DoraMetaStore";

  // These are fields instead of constants because they depend on the call to RocksDB.loadLibrary().
  private final WriteOptions mWriteOption;
  private final ReadOptions mReadOption;
  private final RocksStore mRocksStore;
  private final List<RocksObject> mToClose = new ArrayList<>();

  private final AtomicReference<ColumnFamilyHandle> mFileStatusColumn = new AtomicReference<>();

  /**
   * Creates and initializes a rocks block store.
   *
   * @param baseDir the base directory in which to store inode metadata
   */
  public RocksDBDoraMetaStore(String baseDir) {
    RocksDB.loadLibrary();
    // the rocksDB objects must be initialized after RocksDB.loadLibrary() is called
    mWriteOption = new WriteOptions();
    mReadOption  = new ReadOptions();
    String dbPath = PathUtils.concatPath(baseDir, DORA_META_DB_NAME);
    String backupPath = PathUtils.concatPath(baseDir, DORA_META_DB_NAME + "-backup");

    List<ColumnFamilyDescriptor> columns = new ArrayList<>();
    DBOptions opts = new DBOptions();
    mToClose.add(opts);
    mToClose.add(mReadOption);
    mToClose.add(mWriteOption);

    opts.setAllowConcurrentMemtableWrite(false) // not supported for hash mem tables
            .setCreateMissingColumnFamilies(true)
            .setCreateIfMissing(true)
            .setMaxOpenFiles(-1);
    columns.add(new ColumnFamilyDescriptor(DORA_META_FILE_STATUS_COLUMN.getBytes(),
            new ColumnFamilyOptions()
                    .setMemTableConfig(new HashLinkedListMemTableConfig())
                    .setCompressionType(CompressionType.NO_COMPRESSION)));
    mToClose.addAll(columns.stream().map(
            ColumnFamilyDescriptor::getOptions).collect(Collectors.toList()));

    mRocksStore = new RocksStore(DORA_META_STORE_NAME, dbPath, backupPath, opts, columns,
            Arrays.asList(mFileStatusColumn), false);
  }

  /**
   * Queries the metadata for a file.
   *
   * @param path the full path of this file
   * @return the metadata if found, Optional.empty if not found
   */
  @Override
  public Optional<DoraMeta.FileStatus> getDoraMeta(String path) {
    byte[] status;
    try {
      status = db().get(mFileStatusColumn.get(), path.getBytes());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    if (status == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(DoraMeta.FileStatus.parseFrom(status));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Stores the metadata identified by URI of the file into this RocksDB.
   *
   * @param path the full path of this file
   * @param meta the block metadata
   */
  @Override
  public void putDoraMeta(String path, DoraMeta.FileStatus meta) {
    try {
      db().put(mFileStatusColumn.get(), mWriteOption, path.getBytes(),
              meta.toByteString().toByteArray());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Removes the metadata from the RocksDB. It is not an error if the record specified by the key
   * is not found.
   *
   * @param path the full path of the file whose metadata to be removed
   */
  @Override
  public void removeDoraMeta(String path) {
    try {
      db().delete(mFileStatusColumn.get(), mWriteOption, path.getBytes());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Clears all the metadata in this RocksDB.
   */
  @Override
  public void clear() {
  }

  /**
   * Closes the RocksDb and all resources.
   */
  @Override
  public void close() {
    LOG.info("Closing " + DORA_META_DB_NAME + " and recycling all RocksDB JNI objects");
    // Close the elements in the reverse order they were added
    Collections.reverse(mToClose);
    mToClose.forEach(RocksObject::close);
    mRocksStore.close();
    LOG.info(DORA_META_DB_NAME + " closed");
  }

  /**
   * Queries the estimated number of the records in this RocksDB.
   * Please note, this is not an accurate number.
   *
   * @return the estimated number of records
   */
  @Override
  public long size() {
    try {
      String res = db().getProperty(mFileStatusColumn.get(), "rocksdb.estimate-num-keys");
      long s = Long.parseLong(res);
      return s;
    } catch (org.rocksdb.RocksDBException e) {
      LOG.error("Can not getProperty for rocksdb.estimate-num-keys:" + e);
    }
    return 0;
  }

  private RocksDB db() {
    return mRocksStore.getDb();
  }
}
