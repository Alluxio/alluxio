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

package alluxio.inode;

import static alluxio.master.file.meta.InodeTreeTest.TEST_DIR_MODE;
import static alluxio.master.file.meta.InodeTreeTest.TEST_GROUP;
import static alluxio.master.file.meta.InodeTreeTest.TEST_OWNER;
import static org.mockito.Mockito.mock;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.exception.BlockInfoException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.RpcContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryIdGenerator;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.cross.cluster.CrossClusterPublisher;
import alluxio.master.file.meta.cross.cluster.NoOpCrossClusterPublisher;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.journal.NoopJournalContext;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.caching.CachingInodeStore;
import alluxio.master.metastore.heap.HeapInodeStore;
import alluxio.master.metastore.rocks.RocksInodeStore;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.underfs.UfsManager;

import com.google.common.base.Preconditions;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.function.Consumer;

class InodeBenchBase {
  public static final String HEAP = "heap";
  public static final String ROCKS = "rocks";
  public static final String ROCKSCACHE = "rocksCache";
  private static final CreateDirectoryContext DIRECTORY_CONTEXT = CreateDirectoryContext
      .mergeFrom(CreateDirectoryPOptions.newBuilder().setMode(TEST_DIR_MODE.toProto()))
      .setOwner(TEST_OWNER).setGroup(TEST_GROUP);

  private ArrayList<String> mBasePath;
  private final InodeStore mInodeStore;
  private final InodeTree mTree;
  private final MasterRegistry mRegistry;
  private final BlockMaster mBlockMaster;
  private final InodeLockManager mInodeLockManager = new InodeLockManager();
  private final CrossClusterPublisher mPublisher = new NoOpCrossClusterPublisher();

  InodeBenchBase(String inodeStoreType, String rocksConfig) throws Exception {
    Logger.getRootLogger().setLevel(Level.ERROR);
    mRegistry = new MasterRegistry();
    CoreMasterContext context = MasterTestUtils.testMasterContext();
    MetricsMaster metricsMaster = new MetricsMasterFactory().create(mRegistry, context);
    mRegistry.add(MetricsMaster.class, metricsMaster);
    mBlockMaster = new BlockMasterFactory().create(mRegistry, context);
    InodeDirectoryIdGenerator inodeDirectoryIdGenerator =
        new InodeDirectoryIdGenerator(mBlockMaster);
    UfsManager ufsManager = mock(UfsManager.class);
    MountTable mountTable = new MountTable(ufsManager, mock(MountInfo.class), Clock.systemUTC());
    mInodeStore = getInodeStore(inodeStoreType, rocksConfig, mInodeLockManager);
    mTree = new InodeTree(mInodeStore, mBlockMaster, inodeDirectoryIdGenerator,
        mountTable, mInodeLockManager);
    mRegistry.start(true);
    mTree.initializeRoot(TEST_OWNER, TEST_GROUP, TEST_DIR_MODE, NoopJournalContext.INSTANCE);
  }

  public void after() throws Exception {
    mRegistry.stop();
    mBlockMaster.close();
    mInodeLockManager.close();
    mInodeStore.clear();
    mInodeStore.close();
  }

  static InodeStore getInodeStore(
      String inodeStoreType, String rocksConfig,
      InodeLockManager lockManager) throws IOException {
    switch (inodeStoreType) {
      case HEAP:
        Preconditions.checkArgument(rocksConfig.equals(RocksBenchConfig.JAVA_CONFIG),
            String.format("Heap inode store does not expect a configuration for rocksDB,"
                + " instead should be %s", RocksBenchConfig.JAVA_CONFIG));
        return new HeapInodeStore();
      case ROCKS:
        String dir =
            AlluxioTestDirectory.createTemporaryDirectory("inode-store-bench").getAbsolutePath();
        RocksBenchConfig.setRocksConfig(rocksConfig, dir, Configuration.modifiableGlobal());
        return new RocksInodeStore(dir);
      case ROCKSCACHE:
        dir =
            AlluxioTestDirectory.createTemporaryDirectory("inode-store-bench").getAbsolutePath();
        return new CachingInodeStore(new RocksInodeStore(dir), lockManager);
      default:
        throw new IllegalStateException("Invalid type: " + inodeStoreType);
    }
  }

  // Helper to create a path.
  private void createPath(InodeTree root, AlluxioURI path)
      throws FileAlreadyExistsException, BlockInfoException, InvalidPathException, IOException,
      FileDoesNotExistException {
    try (LockedInodePath inodePath = root.lockInodePath(
        path, InodeTree.LockPattern.WRITE_EDGE, NoopJournalContext.INSTANCE)
    ) {
      root.createPath(RpcContext.NOOP, inodePath, InodeBenchBase.DIRECTORY_CONTEXT, mPublisher);
    }
  }

  void createBasePath(int depth) throws Exception {
    mBasePath = new ArrayList<>(depth + 1);
    String prevBasePath = "/";
    mBasePath.add(prevBasePath);
    for (int i = 0; i < depth; i++) {
      prevBasePath += "nxt/";
      mBasePath.add(prevBasePath);
      createPath(mTree, new AlluxioURI(prevBasePath));
    }
  }

  AlluxioURI getPath(int myId, int depth, long nxtFileId) {
    return new AlluxioURI(String.format("%s%dthread%d",
        mBasePath.get(depth), nxtFileId, myId));
  }

  Inode getFile(int depth, long nxtFileId) throws Exception {
    try (LockedInodePath path = mTree.lockFullInodePath(
        getPath(0, depth, nxtFileId), InodeTree.LockPattern.READ, NoopJournalContext.INSTANCE)) {
      return path.getInode();
    }
  }

  void writeFile(int myId, int depth, long nxtFileId) throws Exception {
    createPath(mTree, getPath(myId, depth, nxtFileId));
  }

  void listDir(int depth, Consumer<Inode> consumeFun) throws Exception {
    try (LockedInodePath path = mTree.lockInodePath(
        new AlluxioURI(mBasePath.get(depth)),
        InodeTree.LockPattern.READ, NoopJournalContext.INSTANCE)) {
      mInodeStore.getChildren(path.getInode().asDirectory()).forEachRemaining(
          consumeFun);
    }
  }
}
