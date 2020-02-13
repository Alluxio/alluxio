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
import alluxio.master.file.meta.EdgeEntry;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.ReadOption;
import alluxio.proto.meta.InodeMeta;
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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.ThreadSafe;

/**
 * File store backed by RocksDB.
 */
@ThreadSafe
public class RocksInodeStore implements InodeStore {
  private static final Logger LOG = LoggerFactory.getLogger(RocksInodeStore.class);
  private static final String INODES_DB_NAME = "inodes";
  private static final String INODES_COLUMN = "inodes";
  private static final String EDGES_COLUMN = "edges";

  // These are fields instead of constants because they depend on the call to RocksDB.loadLibrary().
  private final WriteOptions mDisableWAL;
  private final ReadOptions mReadPrefixSameAsStart;
  private final ReadOptions mIteratorOption;

  private final RocksStore mRocksStore;

  private final AtomicReference<ColumnFamilyHandle> mInodesColumn = new AtomicReference<>();
  private final AtomicReference<ColumnFamilyHandle> mEdgesColumn = new AtomicReference<>();

  /**
   * Creates and initializes a rocks block store.
   *
   * @param baseDir the base directory in which to store inode metadata
   */
  public RocksInodeStore(String baseDir) {
    RocksDB.loadLibrary();
    mDisableWAL = new WriteOptions().setDisableWAL(true);
    mReadPrefixSameAsStart = new ReadOptions().setPrefixSameAsStart(true);
    mIteratorOption = new ReadOptions().setReadaheadSize(
        ServerConfiguration.getBytes(PropertyKey.MASTER_METASTORE_ITERATOR_READAHEAD_SIZE));
    String dbPath = PathUtils.concatPath(baseDir, INODES_DB_NAME);
    String backupPath = PathUtils.concatPath(baseDir, INODES_DB_NAME + "-backup");
    ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
        .setMemTableConfig(new HashLinkedListMemTableConfig())
        .setCompressionType(CompressionType.NO_COMPRESSION)
        .useFixedLengthPrefixExtractor(Longs.BYTES); // We always search using the initial long key
    List<ColumnFamilyDescriptor> columns = Arrays.asList(
        new ColumnFamilyDescriptor(INODES_COLUMN.getBytes(), cfOpts),
        new ColumnFamilyDescriptor(EDGES_COLUMN.getBytes(), cfOpts));
    DBOptions dbOpts = new DBOptions()
        // Concurrent memtable write is not supported for hash linked list memtable
        .setAllowConcurrentMemtableWrite(false)
        .setMaxOpenFiles(-1)
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
    mRocksStore = new RocksStore(dbPath, backupPath, columns, dbOpts,
        Arrays.asList(mInodesColumn, mEdgesColumn));
  }

  @Override
  public void remove(Long inodeId) {
    try {
      byte[] id = Longs.toByteArray(inodeId);
      db().delete(mInodesColumn.get(), mDisableWAL, id);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeInode(MutableInode<?> inode) {
    try {
      db().put(mInodesColumn.get(), mDisableWAL, Longs.toByteArray(inode.getId()),
          inode.toProto().toByteArray());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public WriteBatch createWriteBatch() {
    return new RocksWriteBatch();
  }

  @Override
  public void clear() {
    mRocksStore.clear();
  }

  @Override
  public void addChild(long parentId, String childName, Long childId) {
    try {
      db().put(mEdgesColumn.get(), mDisableWAL, RocksUtils.toByteArray(parentId, childName),
          Longs.toByteArray(childId));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeChild(long parentId, String name) {
    try {
      db().delete(mEdgesColumn.get(), mDisableWAL, RocksUtils.toByteArray(parentId, name));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<MutableInode<?>> getMutable(long id, ReadOption option) {
    byte[] inode;
    try {
      inode = db().get(mInodesColumn.get(), Longs.toByteArray(id));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    if (inode == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(MutableInode.fromProto(InodeMeta.Inode.parseFrom(inode)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterable<Long> getChildIds(Long inodeId, ReadOption option) {
    List<Long> ids = new ArrayList<>();
    try (RocksIterator iter = db().newIterator(mEdgesColumn.get(), mReadPrefixSameAsStart)) {
      iter.seek(Longs.toByteArray(inodeId));
      while (iter.isValid()) {
        ids.add(Longs.fromByteArray(iter.value()));
        iter.next();
      }
    }
    return ids;
  }

  @Override
  public Optional<Long> getChildId(Long inodeId, String name, ReadOption option) {
    byte[] id;
    try {
      id = db().get(mEdgesColumn.get(), RocksUtils.toByteArray(inodeId, name));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    if (id == null) {
      return Optional.empty();
    }
    return Optional.of(Longs.fromByteArray(id));
  }

  @Override
  public Optional<Inode> getChild(Long inodeId, String name, ReadOption option) {
    return getChildId(inodeId, name).flatMap(id -> {
      Optional<Inode> child = get(id);
      if (!child.isPresent()) {
        LOG.warn("Found child edge {}->{}={}, but inode {} does not exist", inodeId, name,
            id, id);
      }
      return child;
    });
  }

  @Override
  public boolean hasChildren(InodeDirectoryView inode, ReadOption option) {
    try (RocksIterator iter = db().newIterator(mEdgesColumn.get(), mReadPrefixSameAsStart)) {
      iter.seek(Longs.toByteArray(inode.getId()));
      return iter.isValid();
    }
  }

  @Override
  public Set<EdgeEntry> allEdges() {
    Set<EdgeEntry> edges = new HashSet<>();
    try (RocksIterator iter = db().newIterator(mEdgesColumn.get())) {
      iter.seekToFirst();
      while (iter.isValid()) {
        long parentId = RocksUtils.readLong(iter.key(), 0);
        String childName = new String(iter.key(), Longs.BYTES, iter.key().length - Longs.BYTES);
        long childId = Longs.fromByteArray(iter.value());
        edges.add(new EdgeEntry(parentId, childName, childId));
        iter.next();
      }
    }
    return edges;
  }

  @Override
  public Set<MutableInode<?>> allInodes() {
    Set<MutableInode<?>> inodes = new HashSet<>();
    try (RocksIterator iter = db().newIterator(mInodesColumn.get())) {
      iter.seekToFirst();
      while (iter.isValid()) {
        inodes.add(getMutable(Longs.fromByteArray(iter.key()), ReadOption.defaults()).get());
        iter.next();
      }
    }
    return inodes;
  }

  /**
   * @return an iterator over stored inodes
   */
  public Iterator<InodeView> iterator() {
    return RocksUtils.createIterator(db().newIterator(mInodesColumn.get(), mIteratorOption),
        (iter) -> getMutable(Longs.fromByteArray(iter.key()), ReadOption.defaults()).get());
  }

  @Override
  public boolean supportsBatchWrite() {
    return true;
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.ROCKS_INODE_STORE;
  }

  @Override
  public void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException {
    mRocksStore.writeToCheckpoint(output);
  }

  @Override
  public void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    mRocksStore.restoreFromCheckpoint(input);
  }

  private class RocksWriteBatch implements WriteBatch {
    private final org.rocksdb.WriteBatch mBatch = new org.rocksdb.WriteBatch();

    @Override
    public void writeInode(MutableInode<?> inode) {
      try {
        mBatch.put(mInodesColumn.get(), Longs.toByteArray(inode.getId()),
            inode.toProto().toByteArray());
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void removeInode(Long key) {
      try {
        mBatch.delete(mInodesColumn.get(), Longs.toByteArray(key));
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void addChild(Long parentId, String childName, Long childId) {
      try {
        mBatch.put(mEdgesColumn.get(), RocksUtils.toByteArray(parentId, childName),
            Longs.toByteArray(childId));
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void removeChild(Long parentId, String childName) {
      try {
        mBatch.delete(mEdgesColumn.get(), RocksUtils.toByteArray(parentId, childName));
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void commit() {
      try {
        db().write(mDisableWAL, mBatch);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      mBatch.close();
    }
  }

  @Override
  public void close() {
    mRocksStore.close();
  }

  private RocksDB db() {
    return mRocksStore.getDb();
  }

  /**
   * @return a newline-delimited string representing the state of the inode store. This is useful
   *         for debugging purposes
   */
  public String toStringEntries() {
    StringBuilder sb = new StringBuilder();
    try (RocksIterator inodeIter =
        db().newIterator(mInodesColumn.get(), new ReadOptions().setTotalOrderSeek(true))) {
      inodeIter.seekToFirst();
      while (inodeIter.isValid()) {
        MutableInode<?> inode;
        try {
          inode = MutableInode.fromProto(InodeMeta.Inode.parseFrom(inodeIter.value()));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        sb.append("Inode " + Longs.fromByteArray(inodeIter.key()) + ": " + inode + "\n");
        inodeIter.next();
      }
    }
    try (RocksIterator edgeIter = db().newIterator(mEdgesColumn.get())) {
      edgeIter.seekToFirst();
      while (edgeIter.isValid()) {
        byte[] key = edgeIter.key();
        byte[] id = new byte[Longs.BYTES];
        byte[] name = new byte[key.length - Longs.BYTES];
        System.arraycopy(key, 0, id, 0, Longs.BYTES);
        System.arraycopy(key, Longs.BYTES, name, 0, key.length - Longs.BYTES);
        sb.append(String.format("<%s,%s>->%s%n", Longs.fromByteArray(id), new String(name),
            Longs.fromByteArray(edgeIter.value())));
        edgeIter.next();
      }
    }
    return sb.toString();
  }
}
