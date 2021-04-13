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

package alluxio.server.tieredstore;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.block.BlockMaster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;
import alluxio.worker.block.allocator.GreedyAllocator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

/**
 * Integration tests for {@link alluxio.worker.block.meta.StorageTier}.
 */
public class TieredStoreIntegrationTest extends BaseIntegrationTest {
  private static final int MEM_CAPACITY_BYTES = 1000;
  private static final WaitForOptions WAIT_OPTIONS =
      WaitForOptions.defaults().setTimeoutMs(2000).setInterval(10);

  private FileSystem mFileSystem;
  private BlockMaster mBlockMaster;
  private SetAttributePOptions mSetPinned;
  private SetAttributePOptions mSetUnpinned;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_RAMDISK_SIZE, MEM_CAPACITY_BYTES)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, 1000)
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, String.valueOf(100))
          .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVEL0_HIGH_WATERMARK_RATIO, 0.8)
          .setProperty(PropertyKey.USER_FILE_RESERVED_BYTES, String.valueOf(100))
          .setProperty(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_ENABLED, String.valueOf(false))
          .setProperty(PropertyKey.WORKER_REVIEWER_CLASS,
              "alluxio.worker.block.reviewer.AcceptingReviewer")
          .build();

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mBlockMaster = mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(BlockMaster.class);
    mSetPinned = SetAttributePOptions.newBuilder().setPinned(true).build();
    mSetUnpinned = SetAttributePOptions.newBuilder().setPinned(false).build();
  }

  /**
   * Tests that deletes go through despite failing initially due to concurrent read.
   */
  @Test
  public void deleteWhileRead() throws Exception {
    int fileSize = MEM_CAPACITY_BYTES / 2; // Small enough not to trigger async eviction

    AlluxioURI file = new AlluxioURI("/test1");
    FileSystemTestUtils.createByteFile(mFileSystem, file, WritePType.MUST_CACHE, fileSize);

    CommonUtils.waitFor("file in memory", () -> {
      try {
        return 100 == mFileSystem.getStatus(file).getInAlluxioPercentage();
      } catch (Exception e) {
        return false;
      }
    }, WAIT_OPTIONS);

    // Open the file
    OpenFilePOptions options =
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE).build();
    FileInStream in = mFileSystem.openFile(file, options);
    Assert.assertEquals(0, in.read());

    // Delete the file
    mFileSystem.delete(file);

    // After the delete, the master should no longer serve the file
    Assert.assertFalse(mFileSystem.exists(file));

    // However, the previous read should still be able to read it as the data still exists
    byte[] res = new byte[fileSize];
    Assert.assertEquals(fileSize - 1, in.read(res, 1, fileSize - 1));
    res[0] = 0;
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(fileSize, res));
    in.close();

    CommonUtils.waitFor("file blocks are deleted" , () -> 0 == mBlockMaster.getUsedBytes(),
        WAIT_OPTIONS);

    // After the file is closed, the master's delete should go through and new files can be created
    AlluxioURI newFile = new AlluxioURI("/test2");
    FileSystemTestUtils.createByteFile(mFileSystem, newFile, WritePType.MUST_CACHE,
        MEM_CAPACITY_BYTES);
    Assert.assertEquals(100, mFileSystem.getStatus(newFile).getInAlluxioPercentage());
  }

  /**
   * Tests that pinning a file prevents it from being evicted.
   */
  @Test
  public void pinFile() throws Exception {
    // Create a file that fills the entire Alluxio store
    AlluxioURI file = new AlluxioURI("/test1");
    // Half of mem capacity to avoid triggering async eviction
    FileSystemTestUtils.createByteFile(mFileSystem, file, WritePType.MUST_CACHE,
        MEM_CAPACITY_BYTES / 2);

    // Pin the file
    mFileSystem.setAttribute(file, mSetPinned);

    // Confirm the pin with master
    Assert.assertTrue(mFileSystem.getStatus(file).isPinned());
    // Try to create a file that cannot be stored unless the previous file is evicted, expect an
    // exception since worker cannot serve the request
    mThrown.expect(Exception.class);
    FileSystemTestUtils.createByteFile(mFileSystem, "/test2", WritePType.MUST_CACHE,
        MEM_CAPACITY_BYTES);
  }

  /**
   * Tests that pinning a file and then unpinning.
   */
  @Test
  public void unpinFile() throws Exception {
    // Create a file that fills the entire Alluxio store
    AlluxioURI file1 = new AlluxioURI("/test1");
    FileSystemTestUtils
        .createByteFile(mFileSystem, file1, WritePType.MUST_CACHE, MEM_CAPACITY_BYTES);

    // Pin the file
    mFileSystem.setAttribute(file1, mSetPinned);
    Assert.assertTrue(mFileSystem.getStatus(file1).isPinned());

    // Unpin the file
    mFileSystem.setAttribute(file1, mSetUnpinned);
    // Confirm the unpin
    Assert.assertFalse(mFileSystem.getStatus(file1).isPinned());

    // Wait until worker receives the new pin-list.
    Thread.sleep(2 * ServerConfiguration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS));

    // Try to create a file that cannot be stored unless the previous file is evicted, this
    // should succeed
    AlluxioURI file2 = new AlluxioURI("/test2");
    FileSystemTestUtils
        .createByteFile(mFileSystem, file2, WritePType.MUST_CACHE, MEM_CAPACITY_BYTES);

    // Wait for validation.
    CommonUtils.waitFor("file2 should be in memory and file1 should be evicted", () -> {
      try {
        return 0 == mFileSystem.getStatus(file1).getInAlluxioPercentage()
            && 100 == mFileSystem.getStatus(file2).getInAlluxioPercentage();
      } catch (Exception e) {
        return false;
      }
    }, WAIT_OPTIONS);
  }

  /**
   * Tests the promotion of a file.
   */
  @Test
  public void promoteBlock() throws Exception {
    AlluxioURI uri1 = new AlluxioURI("/file1");
    AlluxioURI uri2 = new AlluxioURI("/file2");
    AlluxioURI uri3 = new AlluxioURI("/file3");
    FileSystemTestUtils.createByteFile(mFileSystem, uri1, WritePType.CACHE_THROUGH,
        MEM_CAPACITY_BYTES / 3);
    FileSystemTestUtils.createByteFile(mFileSystem, uri2, WritePType.CACHE_THROUGH,
        MEM_CAPACITY_BYTES / 2);
    FileSystemTestUtils.createByteFile(mFileSystem, uri3, WritePType.CACHE_THROUGH,
        MEM_CAPACITY_BYTES / 2);

    AlluxioURI toPromote;
    int toPromoteLen;
    URIStatus file1Info = mFileSystem.getStatus(uri1);
    URIStatus file2Info = mFileSystem.getStatus(uri2);
    URIStatus file3Info = mFileSystem.getStatus(uri3);

    // We know some file will not be in memory, but not which one since we do not want to make
    // any assumptions on the eviction policy
    if (file1Info.getInAlluxioPercentage() < 100) {
      toPromote = uri1;
      toPromoteLen = (int) file1Info.getLength();
      Assert.assertEquals(100, file2Info.getInAlluxioPercentage());
      Assert.assertEquals(100, file3Info.getInAlluxioPercentage());
    } else if (file2Info.getInMemoryPercentage() < 100) {
      toPromote = uri2;
      toPromoteLen = (int) file2Info.getLength();
      Assert.assertEquals(100, file1Info.getInAlluxioPercentage());
      Assert.assertEquals(100, file3Info.getInAlluxioPercentage());
    } else {
      toPromote = uri3;
      toPromoteLen = (int) file3Info.getLength();
      Assert.assertEquals(100, file1Info.getInAlluxioPercentage());
      Assert.assertEquals(100, file2Info.getInAlluxioPercentage());
    }

    FileInStream is = mFileSystem.openFile(toPromote,
        OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build());
    byte[] buf = new byte[toPromoteLen];
    int len = is.read(buf);
    is.close();

    Assert.assertEquals(toPromoteLen, len);
    CommonUtils.waitFor("file in memory", () -> {
      try {
        return 100 == mFileSystem.getStatus(toPromote).getInAlluxioPercentage();
      } catch (Exception e) {
        return false;
      }
    }, WAIT_OPTIONS);
  }

  /**
   * With setting of "StorageDir1, StorageDir2", loading a block from UFS that is bigger than
   * the available space of StorageDir1 should land in StorageDir2 using GreedyAllocator.
   * https://github.com/Alluxio/alluxio/issues/8687
   */
  @Test
  public void greedyAllocator() throws Exception {
    mLocalAlluxioClusterResource.stop();
    int fileLen = 8 * Constants.MB;
    mLocalAlluxioClusterResource
        .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "4MB")
        .setProperty(PropertyKey.WORKER_ALLOCATOR_CLASS, GreedyAllocator.class.getName())
        .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVELS, "1")
        .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH, String.join(",",
            mTempFolder.newFolder("dir1").getAbsolutePath(),
            mTempFolder.newFolder("dir2").getAbsolutePath()))
        .setProperty(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA,
            String.join(",", "2MB", String.valueOf(2 * fileLen)));
    mLocalAlluxioClusterResource.start();
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    AlluxioURI uri1 = new AlluxioURI("/file1");
    FileSystemTestUtils.createByteFile(mFileSystem, uri1, WritePType.THROUGH, fileLen);
    Assert.assertEquals(0, mFileSystem.getStatus(uri1).getInAlluxioPercentage());
    FileSystemTestUtils.loadFile(mFileSystem, uri1.getPath());
    CommonUtils.waitFor("file is in Alluxio", () -> {
      try {
        return 100 == mFileSystem.getStatus(uri1).getInAlluxioPercentage();
      } catch (Exception e) {
        return false;
      }
    }, WAIT_OPTIONS);
  }
}
