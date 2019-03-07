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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.file.meta.EdgeEntry;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.metastore.InodeStore;
import alluxio.proto.meta.InodeMeta;
import alluxio.util.io.FileUtils;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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

  private final AlluxioConfiguration mConf;
  private final String mDbPath;

  private RocksDB mDb;
  private ColumnFamilyHandle mDefaultColumn;
  private ColumnFamilyHandle mInodesColumn;
  private ColumnFamilyHandle mEdgesColumn;

  /**
   * Creates and initializes a rocks block store.
   *
   * @param args inode store arguments
   */
  public RocksInodeStore(InodeStoreArgs args) {
    mConf = args.getConf();
    mDbPath = PathUtils.concatPath(mConf.get(PropertyKey.MASTER_METASTORE_DIR), INODES_DB_NAME);
    RocksDB.loadLibrary();
    mDisableWAL = new WriteOptions().setDisableWAL(true);
    mReadPrefixSameAsStart = new ReadOptions().setPrefixSameAsStart(true);
    try {
      initDb();
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void remove(Long inodeId) {
    try {
      byte[] id = Longs.toByteArray(inodeId);
      mDb.delete(mInodesColumn, mDisableWAL, id);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeInode(MutableInode<?> inode) {
    try {
      mDb.put(mInodesColumn, mDisableWAL, Longs.toByteArray(inode.getId()),
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
    try {
      initDb();
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void addChild(long parentId, String childName, Long childId) {
    try {
      mDb.put(mEdgesColumn, mDisableWAL, RocksUtils.toByteArray(parentId, childName),
          Longs.toByteArray(childId));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeChild(long parentId, String name) {
    try {
      mDb.delete(mEdgesColumn, mDisableWAL, RocksUtils.toByteArray(parentId, name));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<MutableInode<?>> getMutable(long id) {
    byte[] inode;
    try {
      inode = mDb.get(mInodesColumn, Longs.toByteArray(id));
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
  public Iterable<Long> getChildIds(Long inodeId) {
    RocksIterator iter = mDb.newIterator(mEdgesColumn, mReadPrefixSameAsStart);
    iter.seek(Longs.toByteArray(inodeId));
    return () -> new Iterator<Long>() {
      @Override
      public boolean hasNext() {
        return iter.isValid();
      }

      @Override
      public Long next() {
        try {
          return Longs.fromByteArray(iter.value());
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          iter.next();
        }
      }
    };
  }

  @Override
  public Optional<Long> getChildId(Long inodeId, String name) {
    byte[] id;
    try {
      id = mDb.get(mEdgesColumn, RocksUtils.toByteArray(inodeId, name));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    if (id == null) {
      return Optional.empty();
    }
    return Optional.of(Longs.fromByteArray(id));
  }

  @Override
  public Optional<Inode> getChild(Long inodeId, String name) {
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
  public boolean hasChildren(InodeDirectoryView inode) {
    RocksIterator iter = mDb.newIterator(mEdgesColumn, mReadPrefixSameAsStart);
    iter.seek(Longs.toByteArray(inode.getId()));
    return iter.isValid();
  }

  @Override
  public Set<EdgeEntry> allEdges() {
    Set<EdgeEntry> edges = new HashSet<>();
    RocksIterator iter = mDb.newIterator(mEdgesColumn);
    iter.seekToFirst();
    while (iter.isValid()) {
      long parentId = RocksUtils.readLong(iter.key(), 0);
      String childName = new String(iter.key(), Longs.BYTES, iter.key().length - Longs.BYTES);
      long childId = Longs.fromByteArray(iter.value());
      edges.add(new EdgeEntry(parentId, childName, childId));
      iter.next();
    }
    return edges;
  }

  @Override
  public Set<MutableInode<?>> allInodes() {
    Set<MutableInode<?>> inodes = new HashSet<>();
    RocksIterator iter = mDb.newIterator(mInodesColumn);
    iter.seekToFirst();
    while (iter.isValid()) {
      inodes.add(getMutable(Longs.fromByteArray(iter.key())).get());
      iter.next();
    }
    return inodes;
  }

  @Override
  public boolean supportsBatchWrite() {
    return true;
  }

  private class RocksWriteBatch implements WriteBatch {
    private final org.rocksdb.WriteBatch mBatch = new org.rocksdb.WriteBatch();

    @Override
    public void writeInode(MutableInode<?> inode) {
      try {
        mBatch.put(mInodesColumn, Longs.toByteArray(inode.getId()),
            inode.toProto().toByteArray());
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void removeInode(Long key) {
      try {
        mBatch.delete(mInodesColumn, Longs.toByteArray(key));
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void addChild(Long parentId, String childName, Long childId) {
      try {
        mBatch.put(mEdgesColumn, RocksUtils.toByteArray(parentId, childName),
            Longs.toByteArray(childId));
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void removeChild(Long parentId, String childName) {
      try {
        mBatch.delete(mEdgesColumn, RocksUtils.toByteArray(parentId, childName));
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void commit() {
      try {
        mDb.write(mDisableWAL, mBatch);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void initDb() throws RocksDBException {
    if (mDb != null) {
      try {
        // Column handles must be closed before closing the db, or an exception gets thrown.
        mDefaultColumn.close();
        mInodesColumn.close();
        mEdgesColumn.close();
        mDb.close();
      } catch (Throwable t) {
        LOG.error("Failed to close previous rocks database at {}", mDbPath, t);
      }
    }

    Path path = Paths.get(mDbPath);
    try {
      if (Files.exists(path)) {
        FileUtils.deletePathRecursively(mDbPath);
      }
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to clear metastore directory (%s): %s", mDbPath, e.toString()), e);
    }
    try {
      Files.createDirectory(path);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to create metastore directory (%s): %s", mDbPath, e.toString()), e);
    }
    ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
        .setMemTableConfig(new HashLinkedListMemTableConfig())
        .setCompressionType(CompressionType.NO_COMPRESSION)
        .useFixedLengthPrefixExtractor(Longs.BYTES); // We always search using the initial long key

    List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
        new ColumnFamilyDescriptor(INODES_COLUMN.getBytes(), cfOpts),
        new ColumnFamilyDescriptor(EDGES_COLUMN.getBytes(), cfOpts)
    );

    DBOptions options = new DBOptions()
        // Concurrent memtable write is not supported for hash linked list memtable
        .setAllowConcurrentMemtableWrite(false)
        .setMaxOpenFiles(-1)
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);

    // a list which will hold the handles for the column families once the db is opened
    List<ColumnFamilyHandle> columns = new ArrayList<>();
    mDb = RocksDB.open(options, mDbPath, cfDescriptors, columns);
    mDefaultColumn = columns.get(0);
    mInodesColumn = columns.get(1);
    mEdgesColumn = columns.get(2);

    LOG.info("Created new rocks database under path {}", mDbPath);
  }

  @Override
  public void close() {
    mDb.close();
  }

  /**
   * @return a newline-delimited string representing the state of the inode store. This is useful
   *         for debugging purposes
   */
  public String toStringEntries() {
    StringBuilder sb = new StringBuilder();
    RocksIterator inodeIter = mDb.newIterator(mInodesColumn);
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
    inodeIter.close();
    RocksIterator edgeIter = mDb.newIterator(mEdgesColumn);
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
    return sb.toString();
  }
}
