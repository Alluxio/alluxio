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

package alluxio.server.health;

import alluxio.dora.AlluxioURI;
import alluxio.dora.client.file.FileSystem;
import alluxio.dora.client.file.FileSystemTestUtils;
import alluxio.dora.conf.PropertyKey;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioCluster;
import alluxio.dora.master.file.DefaultFileSystemMaster;
import alluxio.dora.master.file.FileSystemMaster;
import alluxio.dora.master.file.RpcContext;
import alluxio.dora.master.file.contexts.DeleteContext;
import alluxio.dora.master.file.meta.InodeTree;
import alluxio.dora.master.file.meta.InodeTree.LockPattern;
import alluxio.dora.master.file.meta.LockedInodePath;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.dora.util.CommonUtils;
import alluxio.dora.util.WaitForOptions;
import alluxio.dora.worker.block.BlockWorker;

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
    FileSystemTestUtils.createByteFile(fs, uri, WritePType.MUST_CACHE, len);
    Assert.assertEquals(1, worker.getStoreMetaFull().getNumberOfBlocks());
    mCluster.stopWorkers();
    fs.delete(uri);
    mCluster.restartMasters();
    mCluster.startWorkers(); // creates a new worker, so need to get the new BlockWorker
    BlockWorker newWorker = mCluster.getWorkerProcess().getWorker(BlockWorker.class);
    CommonUtils.waitFor("orphan blocks to be deleted",
        () -> newWorker.getStoreMetaFull().getNumberOfBlocks() == 0,
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
    FileSystemTestUtils.createByteFile(fs, uri, WritePType.MUST_CACHE, len);
    Assert.assertEquals(1, worker.getStoreMetaFull().getNumberOfBlocks());
    removeFileMetadata(uri);
    mCluster.stopWorkers();
    mCluster.restartMasters();
    mCluster.startWorkers(); // creates a new worker, so need to get the new BlockWorker
    BlockWorker newWorker = mCluster.getWorkerProcess().getWorker(BlockWorker.class);
    CommonUtils.waitFor("invalid blocks to be deleted",
        () -> newWorker.getStoreMetaFull().getNumberOfBlocks() == 0,
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
    FileSystemTestUtils.createByteFile(fs, uri, WritePType.MUST_CACHE, len);
    Assert.assertEquals(1, worker.getStoreMetaFull().getNumberOfBlocks());
    removeFileMetadata(uri);
    CommonUtils.waitFor("invalid blocks to be deleted",
        () -> worker.getStoreMetaFull().getNumberOfBlocks() == 0,
        WaitForOptions.defaults().setTimeoutMs(2000));
  }

  private void removeFileMetadata(AlluxioURI uri) throws Exception {
    FileSystemMaster fsm =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(FileSystemMaster.class);
    InodeTree tree = Whitebox.getInternalState(fsm, "mInodeTree");
    RpcContext rpcContext = ((DefaultFileSystemMaster) fsm).createRpcContext();
    LockedInodePath path
        = tree.lockInodePath(uri, LockPattern.WRITE_EDGE, rpcContext.getJournalContext());
    ((DefaultFileSystemMaster) fsm).deleteInternal(
        rpcContext, path, DeleteContext.defaults(), false);
    path.close();
    rpcContext.close();
  }
}
