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

package alluxio.master.block;

import alluxio.AlluxioURI;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.file.BlockDeletionContext;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.options.DeleteOptions;
import alluxio.master.journal.JournalContext;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.worker.block.BlockWorker;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

public class BlockMasterIntegrityIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder().build();
  private LocalAlluxioCluster mCluster;

  @Before
  public void before() {
    mCluster = mClusterResource.get();
  }

  @Test
  public void deleteOrphanedBlocks() throws Exception {
    AlluxioURI uri = new AlluxioURI("/test");
    int len = 10;
    FileSystem fs = mCluster.getClient();
    BlockWorker worker = mCluster.getWorkerProcess().getWorker(BlockWorker.class);
    FileSystemTestUtils.createByteFile(fs, uri, WriteType.MUST_CACHE, len);
    Assert.assertEquals(1, worker.getStoreMetaFull().getNumberOfBlocks());
    mCluster.stopWorkers();
    fs.delete(uri);
    mCluster.restartMasters();
    mCluster.startWorkers(); // creates a new worker, so need to get the new BlockWorker
    BlockWorker newWorker = mCluster.getWorkerProcess().getWorker(BlockWorker.class);
    CommonUtils.waitFor("orphan blocks to be deleted",
        (v) -> newWorker.getStoreMetaFull().getNumberOfBlocks() == 0,
        WaitForOptions.defaults().setTimeoutMs(2000));
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.MASTER_STARTUP_BLOCK_INTEGRITY_CHECK_ENABLED, "true"
      })
  public void deleteInvalidBlocks() throws Exception {
    AlluxioURI uri = new AlluxioURI("/test");
    int len = 10;
    FileSystem fs = mCluster.getClient();
    BlockWorker worker = mCluster.getWorkerProcess().getWorker(BlockWorker.class);
    FileSystemTestUtils.createByteFile(fs, uri, WriteType.MUST_CACHE, len);
    Assert.assertEquals(1, worker.getStoreMetaFull().getNumberOfBlocks());
    FileSystemMaster fsm =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(FileSystemMaster.class);
    InodeTree tree = Whitebox.getInternalState(fsm, "mInodeTree");
    LockedInodePath path = tree.lockInodePath(uri, InodeTree.LockMode.WRITE);
    DeleteOptions options = DeleteOptions.defaults();
    BlockDeletionContext bctx = Whitebox.invokeMethod(fsm, "createBlockDeletionContext");
    JournalContext jctx = Whitebox.invokeMethod(fsm, "createJournalContext");
    Whitebox.invokeMethod(fsm, "deleteAndJournal", path, options, jctx, bctx);
    path.close();
    jctx.close(); // Journal Context is closed before Block Context
    bctx.close();
    mCluster.stopWorkers();
    mCluster.restartMasters();
    mCluster.startWorkers(); // creates a new worker, so need to get the new BlockWorker
    BlockWorker newWorker = mCluster.getWorkerProcess().getWorker(BlockWorker.class);
    CommonUtils.waitFor("invalid blocks to be deleted",
        (v) -> newWorker.getStoreMetaFull().getNumberOfBlocks() == 0,
        WaitForOptions.defaults().setTimeoutMs(2000));
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.MASTER_PERIODIC_BLOCK_INTEGRITY_CHECK_INTERVAL, "1sec",
      PropertyKey.Name.MASTER_PERIODIC_BLOCK_INTEGRITY_CHECK_REPAIR, "true"
      })
  public void deleteInvalidBlocksPeriodically() throws Exception {
    AlluxioURI uri = new AlluxioURI("/test");
    int len = 10;
    FileSystem fs = mCluster.getClient();
    BlockWorker worker = mCluster.getWorkerProcess().getWorker(BlockWorker.class);
    FileSystemTestUtils.createByteFile(fs, uri, WriteType.MUST_CACHE, len);
    Assert.assertEquals(1, worker.getStoreMetaFull().getNumberOfBlocks());
    FileSystemMaster fsm =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(FileSystemMaster.class);
    InodeTree tree = Whitebox.getInternalState(fsm, "mInodeTree");
    LockedInodePath path = tree.lockInodePath(uri, InodeTree.LockMode.WRITE);
    DeleteOptions options = DeleteOptions.defaults();
    BlockDeletionContext bctx = Whitebox.invokeMethod(fsm, "createBlockDeletionContext");
    JournalContext jctx = Whitebox.invokeMethod(fsm, "createJournalContext");
    Whitebox.invokeMethod(fsm, "deleteAndJournal", path, options, jctx, bctx);
    path.close();
    jctx.close(); // Journal Context is closed before Block Context
    bctx.close();
    CommonUtils.waitFor("invalid blocks to be deleted",
        (v) -> worker.getStoreMetaFull().getNumberOfBlocks() == 0,
        WaitForOptions.defaults().setTimeoutMs(2000));
  }
}
