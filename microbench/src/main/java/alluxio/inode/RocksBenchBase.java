package alluxio.inode;

import alluxio.AlluxioTestDirectory;
import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.file.meta.MutableInodeFile;
import alluxio.master.metastore.rocks.RocksInodeStore;
import alluxio.proto.meta.InodeMeta;

import com.google.common.primitives.Longs;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class RocksBenchBase {

  static final String SER_READ = "serRead";
  static final String NO_SER_READ = "noSerRead";
  static final String SER_NO_ALLOC_READ = "serNoAllocRead";
  static final String NO_SER_NO_ALLOC_READ = "noSerNoAllocRead";

  private final RocksInodeStore mRocksInodeStore;
  private final RocksDB mDB;
  private final AtomicReference<ColumnFamilyHandle> mInodesColumn;
  private final WriteOptions mDisableWAL;

  RocksBenchBase(String confType) throws IOException {
    Logger.getRootLogger().setLevel(Level.ERROR);
    String dir =
        AlluxioTestDirectory.createTemporaryDirectory("inode-store-bench").getAbsolutePath();
    RocksBenchConfig.setRocksConfig(confType, dir, Configuration.modifiableGlobal());
    mRocksInodeStore = new RocksInodeStore(dir);
    Pair<RocksDB, AtomicReference<ColumnFamilyHandle>> dbInfo = mRocksInodeStore.getDBInodeColumn();
    mDB = dbInfo.getFirst();
    mInodesColumn = dbInfo.getSecond();
    mDisableWAL = new WriteOptions().setDisableWAL(true);
  }

  static MutableInode<?> genInode(boolean mIsDirectory) {
    if (mIsDirectory) {
      return MutableInodeFile.create(0, 1, "testFile", System.currentTimeMillis(),
          CreateFileContext.defaults());
    } else {
      return MutableInodeDirectory.create(0, -1, "someParent",
          CreateDirectoryContext.defaults());
    }
  }

  void after() {
    mRocksInodeStore.clear();
    mRocksInodeStore.close();
  }

  long getInodeReadId(long totalCount, long nxtId, int threadCount, int threadId) {
    // since the larger ids were written last (and will be in the memtable), but the zipfan will
    // generate small numbers the most often, we subtract from the total count to read the
    // large ids most often
    // this assumes a workload where latest writes are read more often
    return (totalCount - ((nxtId * threadCount) + threadId) % totalCount) - 1;
  }

  Inode readInode(long totalCount, long nxtId, int threadCount, int threadId) {
    return mRocksInodeStore.get(getInodeReadId(totalCount, nxtId, threadCount, threadId)).get();
  }

  Inode readInode(long totalCount, long nxtId, int threadCount, int threadId, byte[] inodeBytes) {
    return get(getInodeReadId(totalCount, nxtId, threadCount, threadId),
        inodeBytes).map(Inode::wrap).get();
  }

  byte[] readInodeBytes(long totalCount, long nxtId, int threadCount, int threadId) {
    return getBytes(getInodeReadId(totalCount, nxtId, threadCount, threadId)).get();
  }

  int readInodeBytes(long totalCount, long nxtId, int threadCount, int threadId,
                     byte[] inodeBytes) {
    return getBytes(getInodeReadId(totalCount, nxtId, threadCount, threadId), inodeBytes);
  }

  void writeInode(long nxtId, int threadCount, int threadId, MutableInode<?> inode) {
    // we write the bytes directly instead of creating a new inode with the new id
    // since we just want to test the cost of storing the inode and not allocating new ones
    writeBytes(nxtId, threadCount, threadId, inode.toProto().toByteArray());
  }

  void writeBytes(long nxtId, int threadCount, int threadId, byte[] inodeBytes) {
    long nodeId = (nxtId * threadCount) + threadId;
    try {
      mDB.put(mInodesColumn.get(), mDisableWAL, Longs.toByteArray(nodeId),
          inodeBytes);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public int getBytes(long id, byte[] inodeBytes) {
    try {
      return mDB.get(mInodesColumn.get(), Longs.toByteArray(id), inodeBytes);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public Optional<byte[]> getBytes(long id) {
    byte[] inodeBytes;
    try {
      inodeBytes = mDB.get(mInodesColumn.get(), Longs.toByteArray(id));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    if (inodeBytes == null) {
      return Optional.empty();
    }
    return Optional.of(inodeBytes);
  }

  public Optional<MutableInode<?>> get(long id, byte[] inode) {
    int count;
    try {
      count = mDB.get(mInodesColumn.get(), Longs.toByteArray(id), inode);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    if (count == RocksDB.NOT_FOUND) {
      return Optional.empty();
    }
    if (count > inode.length) {
      throw new RuntimeException("Byte array too small to read inode");
    }
    try {
      return Optional.of(MutableInode.fromProto(
          InodeMeta.Inode.parseFrom(ByteBuffer.wrap(inode, 0, count))));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
