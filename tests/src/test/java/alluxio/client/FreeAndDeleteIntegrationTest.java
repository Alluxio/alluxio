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
import alluxio.CommonTestUtils;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
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

import com.google.common.base.Function;
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
public final class FreeAndDeleteIntegrationTest {
  private static final int WORKER_CAPACITY_BYTES = 200 * Constants.MB;
  private static final int USER_QUOTA_UNIT_BYTES = 1000;

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule = new ManuallyScheduleHeartbeat(
      HeartbeatContext.WORKER_BLOCK_SYNC,
      HeartbeatContext.MASTER_LOST_FILES_DETECTION);

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource = new LocalAlluxioClusterResource(
      WORKER_CAPACITY_BYTES, 100 * Constants.MB,
      Constants.USER_FILE_BUFFER_BYTES, Integer.toString(USER_QUOTA_UNIT_BYTES));

  private FileSystem mFileSystem = null;
  private CreateFileOptions mWriteBoth;

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mWriteBoth = StreamOptionUtils.getCreateFileOptionsCacheThrough();
  }

  @Test
  public void freeAndDeleteIntegrationTest() throws Exception {
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 5,
        TimeUnit.SECONDS));
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_FILES_DETECTION, 5,
        TimeUnit.SECONDS));
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    FileOutStream os = mFileSystem.createFile(filePath, mWriteBoth);
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());

    final Long blockId = status.getBlockIds().get(0);
    BlockMaster bm = alluxio.master.PrivateAccess.getBlockMaster(
        mLocalAlluxioClusterResource.get().getMaster().getInternalMaster());
    BlockInfo blockInfo = bm.getBlockInfo(blockId);
    Assert.assertEquals(2, blockInfo.getLength());
    Assert.assertFalse(blockInfo.getLocations().isEmpty());

    final BlockWorker bw = alluxio.worker.PrivateAccess.getBlockWorker(
        mLocalAlluxioClusterResource.get().getWorker());
    Assert.assertTrue(bw.hasBlockMeta(blockId));
    Assert.assertTrue(bm.getLostBlocks().isEmpty());

    mFileSystem.free(filePath);

    // Schedule 1st heartbeat from worker.
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 5,
        TimeUnit.SECONDS));
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);

    // Waiting for the removal of blockMeta from worker.
    CommonTestUtils.waitFor(new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return !bw.hasBlockMeta(blockId);
      }
    }, 100 * Constants.SECOND_MS);

    // Schedule 2nd heartbeat from worker.
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 5,
        TimeUnit.SECONDS));
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);
    // Ensure the 2nd heartbeat is finished.
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 5,
        TimeUnit.SECONDS));

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
    HeartbeatScheduler.schedule(HeartbeatContext.MASTER_LOST_FILES_DETECTION);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_FILES_DETECTION, 5,
        TimeUnit.SECONDS));

    // Verify the blocks are not in mLostBlocks.
    Assert.assertTrue(bm.getLostBlocks().isEmpty());
  }
}
