package alluxio.inode;

import alluxio.AlluxioTestDirectory;
import alluxio.conf.ServerConfiguration;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.file.meta.MutableInodeFile;
import alluxio.master.metastore.rocks.RocksInodeStore;

import java.io.IOException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class RocksBenchBase {

  static final String SER_READ = "serRead";
  static final String NO_SER_READ = "noSerRead";
  static final String SER_NO_ALLOC_READ = "serNoAllocRead";
  static final String NO_SER_NO_ALLOC_READ = "noSerNoAllocRead";

  RocksInodeStore mRocksInodeStore;

  RocksBenchBase(String confType) throws IOException {
    Logger.getRootLogger().setLevel(Level.ERROR);
    String dir =
        AlluxioTestDirectory.createTemporaryDirectory("inode-store-bench").getAbsolutePath();
    RocksBenchConfig.setRocksConfig(confType, dir, ServerConfiguration.global());
    mRocksInodeStore = new RocksInodeStore(dir);
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
    mRocksInodeStore.close();
  }

  Inode readInode(long nxtId, int threadCount, int threadId) {
    long nodeId = (nxtId * threadCount) + threadId;
    return mRocksInodeStore.get(nodeId).get();
  }

  Inode readInode(long nxtId, int threadCount, int threadId, byte[] inodeBytes) {
    long nodeId = (nxtId * threadCount) + threadId;
    return mRocksInodeStore.get(nodeId, inodeBytes).map(Inode::wrap).get();
  }

  byte[] readInodeBytes(long nxtId, int threadCount, int threadId) {
    long nodeId = (nxtId * threadCount) + threadId;
    return mRocksInodeStore.getBytes(nodeId).get();
  }

  int readInodeBytes(long nxtId, int threadCount, int threadId, byte[] inodeBytes) {
    long nodeId = (nxtId * threadCount) + threadId;
    return mRocksInodeStore.getBytes(nodeId, inodeBytes);
  }

  void writeInode(long nxtId, int threadCount, int threadId, MutableInode<?> inode) {
    writeBytes(nxtId, threadCount, threadId, inode.toProto().toByteArray());
  }

  void writeBytes(long nxtId, int threadCount, int threadId, byte[] inodeBytes) {
    long nodeId = (nxtId * threadCount) + threadId;
    mRocksInodeStore.writeInodeBytes(nodeId, inodeBytes);
  }
}
