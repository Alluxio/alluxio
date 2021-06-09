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

package alluxio.client.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.PersistenceState;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.PathUtils;
import alluxio.wire.BlockInfo;
import alluxio.worker.block.BlockWorker;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Integration tests for file free and delete with under storage persisted.
 *
 */
public final class FreeAndDeleteIntegrationTest extends BaseIntegrationTest {
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  private static final int LOCK_POOL_LOW_WATERMARK = 50;
  private static final int LOCK_POOL_HIGH_WATERMARK = 100;
  private static final WaitForOptions WAIT_OPTIONS =
      WaitForOptions.defaults().setTimeoutMs(2000).setInterval(10);

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, USER_QUOTA_UNIT_BYTES)
          .setProperty(PropertyKey.MASTER_LOCK_POOL_LOW_WATERMARK, LOCK_POOL_LOW_WATERMARK)
          .setProperty(PropertyKey.MASTER_LOCK_POOL_HIGH_WATERMARK, LOCK_POOL_HIGH_WATERMARK)
          .build();

  private FileSystem mFileSystem = null;
  private CreateFilePOptions mWriteBoth;

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mWriteBoth = CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH)
        .setRecursive(true).build();
  }

  @Test
  public void freeAndDeleteIntegration() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    FileOutStream os = mFileSystem.createFile(filePath, mWriteBoth);
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    URIStatus status = mFileSystem.getStatus(filePath);
    assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());

    final Long blockId = status.getBlockIds().get(0);
    BlockMaster bm = mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(BlockMaster.class);
    BlockInfo blockInfo = bm.getBlockInfo(blockId);
    assertEquals(2, blockInfo.getLength());
    assertFalse(blockInfo.getLocations().isEmpty());

    final BlockWorker bw =
        mLocalAlluxioClusterResource.get().getWorkerProcess().getWorker(BlockWorker.class);
    assertTrue(bw.hasBlockMeta(blockId));
    assertEquals(0, bm.getLostBlocksCount());

    mFileSystem.free(filePath);

    CommonUtils.waitFor("file is freed", () -> {
      try {
        return 0 == mFileSystem.getStatus(filePath).getInAlluxioPercentage();
      } catch (Exception e) {
        return false;
      }
    }, WAIT_OPTIONS);

    status = mFileSystem.getStatus(filePath);
    // Verify block metadata in master is still present after block freed.
    assertEquals(1, status.getBlockIds().size());
    blockInfo = bm.getBlockInfo(status.getBlockIds().get(0));
    assertEquals(2, blockInfo.getLength());
    // Verify the block has been removed from all workers.
    assertTrue(blockInfo.getLocations().isEmpty());
    assertFalse(bw.hasBlockMeta(blockId));
    // Verify the removed block is added to LostBlocks list.
    assertTrue(bm.isBlockLost(blockInfo.getBlockId()));

    mFileSystem.delete(filePath);

    try {
      // File is immediately gone after delete.
      mFileSystem.getStatus(filePath);
      Assert.fail(String.format("Expected file %s being deleted but it was not.", filePath));
    } catch (FileDoesNotExistException e) {
      // expected
    }

    // Verify the blocks are not in mLostBlocks.
    CommonUtils.waitFor("block is removed from mLostBlocks", () -> {
      try {
        return 0 == bm.getLostBlocksCount();
      } catch (Exception e) {
        return false;
      }
    }, WAIT_OPTIONS);
  }

  /**
   * Tests that deleting a directory with number of files larger than maximum lock cache size will
   * not be blocked.
   */
  @Test(timeout = 3000)
  public void deleteDir() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    for (int file = 0; file < 2 * LOCK_POOL_HIGH_WATERMARK; file++) {
      AlluxioURI filePath = new AlluxioURI(PathUtils.concatPath(uniqPath, "file_" + file));
      mFileSystem.createFile(filePath, CreateFilePOptions.newBuilder().setRecursive(true).build())
          .close();
    }
    mFileSystem.delete(new AlluxioURI(uniqPath),
        DeletePOptions.newBuilder().setRecursive(true).build());
  }
}
