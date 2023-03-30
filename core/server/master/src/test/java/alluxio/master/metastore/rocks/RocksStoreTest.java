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

import static org.junit.Assert.assertArrayEquals;

import alluxio.master.journal.checkpoint.CheckpointInputStream;

import alluxio.resource.LockResource;
import com.google.common.primitives.Longs;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.HashLinkedListMemTableConfig;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksObject;
import org.rocksdb.WriteOptions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class RocksStoreTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Test
  public void backupRestore() throws Exception {
    List<RocksObject> toClose = new ArrayList<>();
    ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
        .setMemTableConfig(new HashLinkedListMemTableConfig())
        .setCompressionType(CompressionType.NO_COMPRESSION)
        .useFixedLengthPrefixExtractor(Longs.BYTES); // We always search using the initial long key
    toClose.add(cfOpts);

    List<ColumnFamilyDescriptor> columnDescriptors =
        Arrays.asList(new ColumnFamilyDescriptor("test".getBytes(), cfOpts));
    String dbDir = mFolder.newFolder("rocks").getAbsolutePath();
    String backupsDir = mFolder.newFolder("rocks-backups").getAbsolutePath();
    AtomicReference<ColumnFamilyHandle> testColumn = new AtomicReference<>();
    DBOptions dbOpts = new DBOptions().setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true)
        .setAllowConcurrentMemtableWrite(false);
    toClose.add(dbOpts);

    RocksStore store =
        new RocksStore("test", dbDir, backupsDir, dbOpts, columnDescriptors,
            Arrays.asList(testColumn));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RocksDB db;
    int count = 10;
    try (RocksReadLock lock = store.checkAndAcquireReadLock()) {
      db = store.getDb();
      for (int i = 0; i < count; i++) {
        db.put(testColumn.get(), new WriteOptions().setDisableWAL(true), ("a" + i).getBytes(),
                "b".getBytes());
      }
    }
    try (RocksWriteLock lock = store.lockForCheckpointing()) {
      store.writeToCheckpoint(baos);
    }
    try (RocksWriteLock lock = store.lockForClosing()) {
      store.close();
    }

    String newBbDir = mFolder.newFolder("rocks-new").getAbsolutePath();
    dbOpts = new DBOptions().setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true)
        .setAllowConcurrentMemtableWrite(false);
    toClose.add(dbOpts);
    store =
        new RocksStore("test-new", newBbDir, backupsDir, dbOpts, columnDescriptors,
            Arrays.asList(testColumn));
    try (RocksWriteLock lock = store.lockForRestoring()) {
      store.restoreFromCheckpoint(
          new CheckpointInputStream(new ByteArrayInputStream(baos.toByteArray())));
    }
    try (RocksReadLock lock = store.checkAndAcquireReadLock()) {
      db = store.getDb();
      for (int i = 0; i < count; i++) {
        assertArrayEquals("b".getBytes(), db.get(testColumn.get(), ("a" + i).getBytes()));
      }
    }
    try (RocksWriteLock lock = store.lockForClosing()) {
      store.close();
    }

    Collections.reverse(toClose);
    toClose.forEach(RocksObject::close);
  }
}
