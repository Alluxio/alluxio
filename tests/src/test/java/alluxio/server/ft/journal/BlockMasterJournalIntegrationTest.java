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

package alluxio.server.ft.journal;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.exception.BlockInfoException;
import alluxio.grpc.WritePType;
import alluxio.master.AlluxioMasterProcess;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.block.BlockMaster;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.wire.WorkerNetAddress;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Integration tests for block master functionality.
 */
public class BlockMasterJournalIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .build();
  private LocalAlluxioCluster mCluster;

  @Before
  public void before() {
    mCluster = mClusterResource.get();
  }

  @Test
  public void journalBlockCreation() throws Exception {
    FileSystem fs = mCluster.getClient();
    BlockMaster blockMaster =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(BlockMaster.class);
    AlluxioURI file = new AlluxioURI("/test");
    FileSystemTestUtils.createByteFile(fs, file, WritePType.MUST_CACHE, 10);
    URIStatus status = fs.getStatus(file);
    Long blockId = status.getBlockIds().get(0);
    assertNotNull(blockMaster.getBlockInfo(blockId));
    mCluster.stopMasters();
    mCluster.startMasters();
    AlluxioMasterProcess masterProcess = mCluster.getLocalAlluxioMaster().getMasterProcess();
    assertNotNull(masterProcess.getMaster(BlockMaster.class).getBlockInfo(blockId));
  }

  @Test
  public void journalBlockDeletion() throws Exception {
    FileSystem fs = mCluster.getClient();
    BlockMaster blockMaster =
        mCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(BlockMaster.class);
    AlluxioURI file = new AlluxioURI("/test");
    FileSystemTestUtils.createByteFile(fs, file, WritePType.MUST_CACHE, 10);
    URIStatus status = fs.getStatus(file);
    Long blockId = status.getBlockIds().get(0);
    assertNotNull(blockMaster.getBlockInfo(blockId));
    fs.delete(file);
    WorkerNetAddress workerAddress = mCluster.getWorkerAddress();
    try {
      blockMaster.getBlockInfo(blockId);
      fail("Expected the block to be deleted");
    } catch (BlockInfoException e) {
      // expected
    }
    mCluster.stopMasters();
    mCluster.startMasters();
    AlluxioMasterProcess masterProcess = mCluster.getLocalAlluxioMaster().getMasterProcess();
    try {
      masterProcess.getMaster(BlockMaster.class).getBlockInfo(blockId);
      fail("Expected the block to be deleted after restart");
    } catch (BlockInfoException e) {
      // expected
    }
  }
}
