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

import alluxio.master.file.meta.EdgeEntry;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryView;
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

  private final RocksStore mRocksStore;

  private final AtomicReference<ColumnFamilyHandle> mInodesColumn = new AtomicReference<>();
  private final AtomicReference<ColumnFamilyHandle> mEdgesColumn = new AtomicReference<>();
  private final AtomicReference<ColumnFamilyHandle> mIndicesColumn = new AtomicReference<>();

  /**
   * Creates and initializes a rocks block store.
   *
   * @param baseDir the base directory in which to store inode metadata
   */
  public RocksInodeStore(String baseDir) {
    RocksDB.loadLibrary();
    mDisableWAL = new WriteOptions().setDisableWAL(true);
    mReadPrefixSameAsStart = new ReadOptions().setPrefixSameAsStart(true);
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
        Arrays.asList(mInodesColumn, mEdgesColumn, mIndicesColumn));
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

  @Override
  public void setIndice(long id, InodeIndice indice, boolean isSet) {
    // Create composite key for the given indice and id
    byte[] indiceKey = RocksUtils.toByteArray(indice.getIndiceId(), id);
    try {
      if (isSet) {
        db().put(indiceKey, Longs.toByteArray(id));
      } else {
        db().delete(indiceKey);
      }
    } catch (RocksDBException rexc) {
      throw new RuntimeException(rexc);
    }
  }

  @Override
  public void clearIndices(InodeIndice indice) {
    try {
      db().deleteRange(RocksUtils.toByteArray(indice.getIndiceId(), 0L),
          RocksUtils.toByteArray(indice.getIndiceId() + 1, 0L));
    } catch (RocksDBException rexc) {
      throw new RuntimeException(rexc);
    }
  }

  @Override
  public Iterator<Long> getIndiced(InodeIndice indice) {
    return new RocksIndiceIterator(db().newIterator(mIndicesColumn.get()), indice);
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

  /**
   * Used to implement an indice iterator off of RocksIterator.
   * This will assume the underlying Rocks table is following below composite
   * key convention for identifying indices.
   *
   * {Indice-Id + Inode-Id} -> {Inode-Id}
   *
   * Starting the composite key with an IndiceId provides range query capabilities.
   * For exmaple to iterate all keys with Indice-Id(1), we would need to query
   * starting from 100000000... and end at 200000000..., assuming inode Ids start from 0.
   *
   */
  class RocksIndiceIterator implements Iterator<Long> {

    /** Underlying Rocks iterator for the indice table.  */
    private RocksIterator mRocksIterator;
    /**  */
    private InodeIndice mIndice;

    public RocksIndiceIterator(RocksIterator rocksIterator, InodeStore.InodeIndice indice) {
      mIndice = indice;
      mRocksIterator = rocksIterator;
      // Start iteration from the first possible key for given indice.
      mRocksIterator.seek(RocksUtils.toByteArray(indice.getIndiceId(), 0L));
    }

    @Override
    public boolean hasNext() {
      // Iterator is still valid and its key hasn't crossed to the next indice space.
      return mRocksIterator.isValid() && (mRocksIterator.key()[0] <= mIndice.getIndiceId() + 1);
    }

    @Override
    public Long next() {
      try {
        return Longs.fromByteArray(mRocksIterator.value());
      } finally {
        mRocksIterator.next();
      }
    }
  }
}
