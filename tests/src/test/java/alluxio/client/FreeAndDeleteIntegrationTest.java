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

package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.IntegrationTestUtils;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.BaseIntegrationTest;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.FileDoesNotExistException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.PersistenceState;
import alluxio.util.io.PathUtils;
import alluxio.wire.BlockInfo;
import alluxio.worker.block.BlockWorker;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Integration tests for file free and delete with under storage persisted.
 *
 */
public final class FreeAndDeleteIntegrationTest extends BaseIntegrationTest {
  private static final int USER_QUOTA_UNIT_BYTES = 1000;

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule = new ManuallyScheduleHeartbeat(
      HeartbeatContext.WORKER_BLOCK_SYNC,
      HeartbeatContext.MASTER_LOST_FILES_DETECTION);

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, USER_QUOTA_UNIT_BYTES)
          .build();

  private FileSystem mFileSystem = null;
  private CreateFileOptions mWriteBoth;

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mWriteBoth = CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH);
  }

  @Test
  public void freeAndDeleteIntegration() throws Exception {
    HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 5, TimeUnit.SECONDS);
    HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_FILES_DETECTION, 5, TimeUnit.SECONDS);
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    FileOutStream os = mFileSystem.createFile(filePath, mWriteBoth);
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());

    final Long blockId = status.getBlockIds().get(0);
    BlockMaster bm = mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(BlockMaster.class);
    BlockInfo blockInfo = bm.getBlockInfo(blockId);
    Assert.assertEquals(2, blockInfo.getLength());
    Assert.assertFalse(blockInfo.getLocations().isEmpty());

    final BlockWorker bw =
        mLocalAlluxioClusterResource.get().getWorkerProcess().getWorker(BlockWorker.class);
    Assert.assertTrue(bw.hasBlockMeta(blockId));
    Assert.assertTrue(bm.getLostBlocks().isEmpty());

    mFileSystem.free(filePath);

    IntegrationTestUtils.waitForBlocksToBeFreed(bw, blockId);

    status = mFileSystem.getStatus(filePath);
    // Verify block metadata in master is still present after block freed.
    Assert.assertEquals(1, status.getBlockIds().size());
    blockInfo = bm.getBlockInfo(status.getBlockIds().get(0));
    Assert.assertEquals(2, blockInfo.getLength());
    // Verify the block has been removed from all workers.
    Assert.assertTrue(blockInfo.getLocations().isEmpty());
    Assert.assertFalse(bw.hasBlockMeta(blockId));
    // Verify the removed block is added to LostBlocks list.
    Assert.assertTrue(bm.getLostBlocks().contains(blockInfo.getBlockId()));

    mFileSystem.delete(filePath);

    try {
      // File is immediately gone after delete.
      mFileSystem.getStatus(filePath);
      Assert.fail(String.format("Expected file %s being deleted but it was not.", filePath));
    } catch (FileDoesNotExistException e) {
      // expected
    }

    // Execute the lost files detection.
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_LOST_FILES_DETECTION);

    // Verify the blocks are not in mLostBlocks.
    Assert.assertTrue(bm.getLostBlocks().isEmpty());
  }
}
