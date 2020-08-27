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
import org.rocksdb.WriteOptions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class RocksStoreTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Test
  public void backupRestore() throws Exception {
    ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
        .setMemTableConfig(new HashLinkedListMemTableConfig())
        .setCompressionType(CompressionType.NO_COMPRESSION)
        .useFixedLengthPrefixExtractor(Longs.BYTES); // We always search using the initial long key
    DBOptions dbOpts = new DBOptions()
        // Concurrent memtable write is not supported for hash linked list memtable
        .setAllowConcurrentMemtableWrite(false)
        .setMaxOpenFiles(-1)
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);

    List<ColumnFamilyDescriptor> columnDescriptors =
        Arrays.asList(new ColumnFamilyDescriptor("test".getBytes(), cfOpts));
    String dbDir = mFolder.newFolder("rocks").getAbsolutePath();
    String backupsDir = mFolder.newFolder("rocks-backups").getAbsolutePath();
    AtomicReference<ColumnFamilyHandle> testColumn = new AtomicReference<>();
    RocksStore store =
        new RocksStore(dbDir, backupsDir, columnDescriptors, dbOpts, Arrays.asList(testColumn));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RocksDB db = store.getDb();
    int count = 10;
    for (int i = 0; i < count; i++) {
      db.put(testColumn.get(), new WriteOptions().setDisableWAL(true), ("a" + i).getBytes(),
          "b".getBytes());
    }
    store.writeToCheckpoint(baos);

    String newBbDir = mFolder.newFolder("rocks-new").getAbsolutePath();
    store =
        new RocksStore(newBbDir, backupsDir, columnDescriptors, dbOpts, Arrays.asList(testColumn));
    store.restoreFromCheckpoint(
        new CheckpointInputStream(new ByteArrayInputStream(baos.toByteArray())));
    db = store.getDb();
    for (int i = 0; i < count; i++) {
      assertArrayEquals("b".getBytes(), db.get(testColumn.get(), ("a" + i).getBytes()));
    }
  }
}
