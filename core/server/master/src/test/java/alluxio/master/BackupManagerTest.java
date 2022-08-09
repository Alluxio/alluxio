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

package alluxio.master;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.AlluxioTestDirectory;
import alluxio.clock.ManualClock;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.DefaultBlockMaster;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectory;
import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.file.meta.MutableInodeFile;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.journal.noop.NoopJournalSystem;
import alluxio.master.metastore.BlockMetaStore;
import alluxio.master.metastore.heap.HeapBlockMetaStore;
import alluxio.master.metastore.heap.HeapInodeStore;
import alluxio.master.metastore.rocks.RocksBlockMetaStore;
import alluxio.master.metastore.rocks.RocksInodeStore;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.proto.meta.Block;
import alluxio.proto.meta.InodeMeta;
import alluxio.resource.CloseableIterator;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit tests for the {@link BackupManager} class.
 * This is intentionally put in alluxio-core-server module instead of
 * alluxio-core-common due to the imports.
 */
public class BackupManagerTest {
  private BlockMaster mBlockMaster;
  private MasterRegistry mRegistry;
  private ManualClock mClock;
  private ExecutorService mExecutorService;
  private MetricsMaster mMetricsMaster;

  /**
   * Sets up the dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    mRegistry = new MasterRegistry();
    mClock = new ManualClock();
    mExecutorService =
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("TestBlockMaster-%d", true));
  }

  /**
   * Stops the master after a test ran.
   */
  @After
  public void after() throws Exception {
    mRegistry.stop();
  }

  private BlockMetaStore.Block createNewBlock(long blockId) {
    Block.BlockMeta meta = Block.BlockMeta.newBuilder().setLength(1000).build();
    return new BlockMetaStore.Block(blockId, meta);
  }

  private MutableInodeFile createNewFile(long fileId) {
    InodeMeta.InodeOrBuilder builder = InodeMeta.Inode.newBuilder()
        .setId(fileId).setPersistenceState(PersistenceState.PERSISTED.toString());
    return MutableInodeFile.fromProto(builder);
  }

  private MutableInodeDirectory createRootDir(long fileId) {
    InodeMeta.InodeOrBuilder builder = InodeMeta.Inode.newBuilder()
        .setId(fileId)
        .setPersistenceState(PersistenceState.PERSISTED.toString())
        .setIsDirectory(true)
        .setDefaultAcl(
            alluxio.proto.shared.Acl.AccessControlList.newBuilder().setIsDefault(true).build());
    return MutableInodeDirectory.fromProto(builder);
  }

  @Test
  public void rocksBlockStoreIteratorClosed() throws Exception {
    // Prepare some data for the iterator
    List<BlockMetaStore.Block> blocks = new ArrayList<>();
    blocks.add(createNewBlock(1L));
    blocks.add(createNewBlock(2L));
    blocks.add(createNewBlock(3L));
    // When RocksBlockStore.iterator(), return mock iterator
    AtomicBoolean blockIteratorClosed = new AtomicBoolean(false);
    CloseableIterator<BlockMetaStore.Block> testBlockIter =
        CloseableIterator.create(blocks.iterator(), (whatever) -> blockIteratorClosed.set(true));
    RocksBlockMetaStore mockBlockStore = mock(RocksBlockMetaStore.class);
    when(mockBlockStore.getCloseableIterator()).thenReturn(testBlockIter);

    // Prepare the BlockMaster for the backup operation
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext(
        new NoopJournalSystem(), null,
        () -> mockBlockStore,
        x -> new HeapInodeStore());
    mMetricsMaster = new MetricsMasterFactory().create(mRegistry, masterContext);
    mBlockMaster = new DefaultBlockMaster(mMetricsMaster, masterContext, mClock,
            ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
    mRegistry.add(BlockMaster.class, mBlockMaster);
    mRegistry.start(true);

    // Perform the backup operation
    BackupManager manager = new BackupManager(mRegistry);
    File backupDir = AlluxioTestDirectory.createTemporaryDirectory("backup-dir");
    File backupFile = new File(backupDir, "1.backup");
    AtomicLong counter = new AtomicLong(0L);
    manager.backup(new FileOutputStream(backupFile), counter);

    // verify iterators all closed properly
    assertTrue(blockIteratorClosed.get());
  }

  @Test
  public void rocksInodeStoreIteratorNotUsed() throws Exception {
    // Prepare some data for the iterator
    List<InodeView> inodes = new ArrayList<>();
    inodes.add(createRootDir(99L));
    inodes.add(createNewFile(100L));
    inodes.add(createNewFile(101L));
    inodes.add(createNewFile(102L));

    // When RocksInodeStore.iterator(), return mock iterator
    RocksInodeStore mockInodeStore = mock(RocksInodeStore.class);
    // Make sure the iterator is not used in the backup operation
    when(mockInodeStore.getCloseableIterator()).thenThrow(new UnsupportedOperationException());
    // Return the list of children when iterating the root
    Mockito.doAnswer((ignored) -> CloseableIterator.noopCloseable(
        inodes.stream().skip(1).map(Inode::wrap).iterator()))
        .when(mockInodeStore).getChildren(eq(Inode.wrap(inodes.get(0)).asDirectory()));
    // When the root inode is asked for, return the directory
    InodeView dir = inodes.get(0);
    when(mockInodeStore.get(eq(0L)))
        .thenReturn(Optional.of(new InodeDirectory((InodeDirectoryView) dir)));

    CoreMasterContext masterContext = MasterTestUtils.testMasterContext(
        new NoopJournalSystem(), null,
        HeapBlockMetaStore::new,
        x -> mockInodeStore);
    mMetricsMaster = new MetricsMasterFactory().create(mRegistry, masterContext);
    mBlockMaster = new DefaultBlockMaster(mMetricsMaster, masterContext, mClock,
        ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
    mRegistry.add(BlockMaster.class, mBlockMaster);

    // Prepare the FileSystemMaster for the backup operation
    FileSystemMaster fsMaster = new DefaultFileSystemMaster("ClusterId",
        mBlockMaster, masterContext,
        ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
    mRegistry.add(FileSystemMaster.class, fsMaster);
    mRegistry.start(true);

    // Finish backup operation
    BackupManager manager = new BackupManager(mRegistry);
    File backupDir = AlluxioTestDirectory.createTemporaryDirectory("backup-dir");
    File backupFile = new File(backupDir, "1.backup");
    AtomicLong counter = new AtomicLong(0L);
    // No exception means the RocksInodeStore iterator is not used
    manager.backup(new FileOutputStream(backupFile), counter);
  }
}
